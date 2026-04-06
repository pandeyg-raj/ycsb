/*
 * trace_driver.c
 *
 * Twitter cache trace replay driver for Cassandra / LEAST.
 *
 * Reads a Twitter Twemcache trace (plain text CSV), maps:
 *   GET  →  SELECT field0 FROM ycsb.usertable WHERE y_id = ?
 *   SET  →  INSERT INTO ycsb.usertable (y_id, field0) VALUES (?, ?)
 *
 * YCSB schema (must exist before running):
 *   CREATE TABLE ycsb.usertable (
 *       y_id   varchar,
 *       field0 varchar,
 *       PRIMARY KEY (y_id)
 *   ) WITH caching     = {'keys':'NONE','rows_per_partition':'none'}
 *     AND  compression = {'enabled':false}
 *     AND  read_repair = 'NONE';
 *
 * Build:
 *   gcc -O2 -o trace_driver trace_driver.c -lcassandra -lpthread -lm
 *
 * Usage:
 *   # Load phase (populate database, no timing)
 *   ./trace_driver --trace cluster46.0 --host 10.10.1.1 \
 *                  --least --disable-ttl --speed 0 --output /dev/null
 *
 *   # Benchmark phase (timed measurement)
 *   ./trace_driver --trace cluster46.0 --host 10.10.1.1 \
 *                  --least --disable-ttl --speed 1.0 \
 *                  --duration 300 --output least_c46.csv
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>    /* strcasecmp */
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <math.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <getopt.h>

#include <cassandra.h>

/* ── Constants ──────────────────────────────────────────────────────────────── */

#define MAX_KEY_LEN     256
#define QUEUE_CAPACITY  8192           /* must be power of 2 */
#define REPORT_INTERVAL 10             /* seconds between progress prints */
#define MAX_SAMPLES     2000000        /* per-thread latency samples */

/* Maximum value size found in the trace — set during prescan, used to
 * allocate per-thread value buffers. No arbitrary cap, no truncation. */
static int g_max_value_size = 0;

/* ── Operation type ─────────────────────────────────────────────────────────── */

typedef enum { OP_GET = 0, OP_SET = 1, OP_SKIP = 2 } OpType;

/* ── Trace record ───────────────────────────────────────────────────────────── */

typedef struct {
    uint64_t timestamp;            /* seconds since epoch — used for pacing */
    char     key[MAX_KEY_LEN];
    int      value_size;           /* bytes — used to size the INSERT value */
    OpType   op;
} TraceRecord;

/* ── Latency tracker (per thread, no locking) ───────────────────────────────── */

typedef struct {
    double   *samples;
    uint64_t  count;
    uint64_t  capacity;
    double    sum_us;
    uint64_t  errors;
    uint64_t  timeouts;
} Tracker;

static void tracker_init(Tracker *t) {
    t->capacity = MAX_SAMPLES;
    t->samples  = malloc(MAX_SAMPLES * sizeof(double));
    t->count    = 0;
    t->sum_us   = 0.0;
    t->errors   = 0;
    t->timeouts = 0;
    if (!t->samples) { perror("malloc tracker"); exit(1); }
}

static void tracker_free(Tracker *t) { free(t->samples); }

static inline void tracker_record(Tracker *t, double us) {
    t->sum_us += us;
    if (t->count < t->capacity) {
        t->samples[t->count] = us;
    } else {
        /* reservoir sampling so we keep a representative set */
        uint64_t idx = (uint64_t)rand() % (t->count + 1);
        if (idx < t->capacity)
            t->samples[idx] = us;
    }
    t->count++;
}

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

/* Call only once — sorts samples in place */
static double tracker_pct(Tracker *t, double pct) {
    if (!t->count) return 0.0;
    uint64_t n = t->count < t->capacity ? t->count : t->capacity;
    qsort(t->samples, n, sizeof(double), cmp_double);
    uint64_t idx = (uint64_t)(n * pct / 100.0);
    if (idx >= n) idx = n - 1;
    return t->samples[idx];
}

static double tracker_mean(Tracker *t) {
    return t->count ? t->sum_us / t->count : 0.0;
}

/* ── Lock-free ring queue (single producer, multi consumer) ─────────────────── */

typedef struct {
    TraceRecord *buf;
    uint64_t     head;      /* producer index */
    uint64_t     tail;      /* consumer index */
    uint64_t     mask;      /* capacity - 1 */
    volatile int done;      /* set when trace file is exhausted */
} RingQueue;

static void queue_init(RingQueue *q, uint64_t cap) {
    q->buf  = malloc(cap * sizeof(TraceRecord));
    q->head = q->tail = 0;
    q->mask = cap - 1;
    q->done = 0;
    if (!q->buf) { perror("malloc queue"); exit(1); }
}

static inline bool queue_push(RingQueue *q, const TraceRecord *r) {
    uint64_t next = (q->head + 1) & q->mask;
    if (next == __atomic_load_n(&q->tail, __ATOMIC_ACQUIRE))
        return false;  /* full */
    q->buf[q->head & q->mask] = *r;
    __atomic_store_n(&q->head, q->head + 1, __ATOMIC_RELEASE);
    return true;
}

static inline bool queue_pop(RingQueue *q, TraceRecord *out) {
    uint64_t tail = q->tail;
    if (tail == __atomic_load_n(&q->head, __ATOMIC_ACQUIRE))
        return false;  /* empty */
    *out = q->buf[tail & q->mask];
    __atomic_store_n(&q->tail, tail + 1, __ATOMIC_RELEASE);
    return true;
}

/* ── Timing ─────────────────────────────────────────────────────────────────── */

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static inline void sleep_ns(uint64_t ns) {
    struct timespec ts = { (time_t)(ns / 1000000000ULL),
                           (long)(ns % 1000000000ULL) };
    nanosleep(&ts, NULL);
}

/* ── Config ─────────────────────────────────────────────────────────────────── */

/*
 * Keyspace, table, and replication factor are hardcoded to match the
 * exact schema from the original program:
 *   keyspace = ycsb, table = usertable, RF = 5, DURABLE_WRITES = true
 */
typedef struct {
    const char *trace_path;
    const char *hosts;          /* coordinator node IP */
    int         port;
    int         num_threads;
    int         duration_sec;
    double      speed;          /* replay speed vs real-time; 0 = max rate */
    const char *output_csv;
    int         least_mode;     /* 1 = prepend 0x00 to field0 (LEAST EC trigger) */
    int         disable_ttl;    /* 1 = write all data permanently (no TTL expiry) */
    int         full_trace;     /* 1 = run until trace file ends, ignore --duration */
    const char *consistency;
    int         load_mode;      /* 1 = skip GETs, only execute SETs (load phase) */
    int         load_buffer;    /* seconds beyond duration to include in load (-1 = not set) */
    const char *preload_file;   /* path to file of keys to pre-insert before benchmark */
    /* ── manual mode ── */
    int         manual;         /* 1 = ignore trace, use synthetic workload */
    uint64_t    num_keys;       /* number of unique keys (load writes all, bench reads all) */
    int         value_size;     /* fixed value size in bytes for manual mode */
    double      read_ratio;     /* fraction of ops that are GETs in benchmark (0.0-1.0) */
} Config;

static Config cfg = {
    .trace_path   = NULL,
    .hosts        = "10.10.1.1",
    .port         = 9042,
    .num_threads  = 32,
    .duration_sec = 3600,
    .speed        = 1.0,
    .output_csv   = "latency.csv",
    .least_mode   = 0,
    .disable_ttl  = 0,
    .full_trace   = 0,
    .consistency  = "ONE",
    .load_mode    = 0,
    .load_buffer  = -1,
    .preload_file = NULL,
    .manual       = 0,
    .num_keys     = 1000000,    /* 1M keys default */
    .value_size   = 1024,       /* 1 KB default */
    .read_ratio   = 0.5,        /* 50/50 default */
};

/* ── Shared globals ──────────────────────────────────────────────────────────── */

static volatile int  g_stop = 0;
static RingQueue     g_queue;
static CassCluster  *g_cluster  = NULL;
static CassSession  *g_session  = NULL;
static const CassPrepared *g_select = NULL;
static const CassPrepared *g_insert = NULL;

/* Per-thread state */
typedef struct { Tracker get; Tracker set; } ThreadStats;
static ThreadStats  *g_stats    = NULL;
static char        **g_vbufs    = NULL;   /* value buffers, one per thread */

/* Progress tracking — written by reader, read by reporter */
static volatile uint64_t g_total_lines  = 0;
static volatile uint64_t g_total_gets   = 0;
static volatile uint64_t g_total_sets   = 0;

/* Manual mode: atomic counter for next key to write during load */
static volatile uint64_t g_next_key     = 0;

/* ── Cassandra setup ─────────────────────────────────────────────────────────── */

static void cass_die(const char *ctx, CassFuture *f) {
    const char *msg; size_t len;
    cass_future_error_message(f, &msg, &len);
    fprintf(stderr, "ERROR [%s]: %.*s\n", ctx, (int)len, msg);
    cass_future_free(f);
    exit(1);
}

static CassConsistency parse_consistency(const char *s) {
    if (!strcasecmp(s, "ONE"))           return CASS_CONSISTENCY_ONE;
    if (!strcasecmp(s, "TWO"))           return CASS_CONSISTENCY_TWO;
    if (!strcasecmp(s, "THREE"))         return CASS_CONSISTENCY_THREE;
    if (!strcasecmp(s, "QUORUM"))        return CASS_CONSISTENCY_QUORUM;
    if (!strcasecmp(s, "ALL"))           return CASS_CONSISTENCY_ALL;
    if (!strcasecmp(s, "LOCAL_QUORUM"))  return CASS_CONSISTENCY_LOCAL_QUORUM;
    if (!strcasecmp(s, "LOCAL_ONE"))     return CASS_CONSISTENCY_LOCAL_ONE;
    fprintf(stderr, "Unknown consistency level: %s\n"
                    "Valid: ONE TWO THREE QUORUM ALL LOCAL_QUORUM LOCAL_ONE\n", s);
    exit(1);
}

static void cass_connect(void) {
    g_cluster = cass_cluster_new();
    g_session = cass_session_new();

    cass_cluster_set_contact_points(g_cluster, cfg.hosts);
    cass_cluster_set_port(g_cluster, cfg.port);
    cass_cluster_set_num_threads_io(g_cluster, 4);
    cass_cluster_set_queue_size_io(g_cluster, 65536);
    cass_cluster_set_consistency(g_cluster, parse_consistency(cfg.consistency));

    /*
     * Pin the coordinator to the specified host.
     * Matches: cass_cluster_set_whitelist_filtering(cluster, "10.10.1.2")
     * from the original program — ensures all requests go through
     * this node so we're measuring a single known coordinator.
     */
    cass_cluster_set_whitelist_filtering(g_cluster, cfg.hosts);

    /* Step 1: connect without a keyspace first (same as original program) */
    CassFuture *f = cass_session_connect(g_session, g_cluster);
    cass_future_wait(f);
    if (cass_future_error_code(f) != CASS_OK)
        cass_die("connect", f);
    cass_future_free(f);
    printf("Connected to Cassandra cluster!\n");

    /* Step 2: create keyspace — exact query from original program */
    {
        const char *q =
            "CREATE KEYSPACE IF NOT EXISTS ycsb "
            "WITH replication = {"
            "  'class': 'SimpleStrategy',"
            "  'replication_factor': '5'"
            "} AND DURABLE_WRITES = true;";
        f = cass_session_execute(g_session, cass_statement_new(q, 0));
        cass_future_wait(f);
        if (cass_future_error_code(f) != CASS_OK) {
            const char *msg; size_t len;
            cass_future_error_message(f, &msg, &len);
            fprintf(stderr, "Failed to create keyspace: %.*s\n", (int)len, msg);
            /* non-fatal — keyspace may already exist */
        }
        cass_future_free(f);
        fprintf(stderr, "Create keyspace: Success, waiting 5s to propagate\n");
        sleep(5);
    }

    /* Step 3: create table — exact query from original program */
    {
        const char *q =
            "CREATE TABLE IF NOT EXISTS ycsb.usertable ("
            "  y_id   varchar,"
            "  field0 varchar,"
            "  PRIMARY KEY (y_id)"
            ") WITH caching = { 'keys' : 'NONE', 'rows_per_partition' : 'none' }"
            "  AND compression = { 'enabled' : false }"
            "  AND read_repair = 'NONE';";
        f = cass_session_execute(g_session, cass_statement_new(q, 0));
        cass_future_wait(f);
        if (cass_future_error_code(f) != CASS_OK) {
            const char *msg; size_t len;
            cass_future_error_message(f, &msg, &len);
            fprintf(stderr, "Failed to create table: %.*s\n", (int)len, msg);
            /* non-fatal — table may already exist */
        }
        cass_future_free(f);
        fprintf(stderr, "Create table: Success, waiting 10s to propagate\n");
        sleep(10);
    }

    /* Step 4: switch session to ycsb keyspace */
    {
        f = cass_session_execute(g_session,
                cass_statement_new("USE ycsb", 0));
        cass_future_wait(f);
        if (cass_future_error_code(f) != CASS_OK)
            cass_die("USE ycsb", f);
        cass_future_free(f);
    }

    /* Step 5: prepare SELECT and INSERT */
    {
        f = cass_session_prepare(g_session,
                "SELECT field0 FROM usertable WHERE y_id = ?");
        cass_future_wait(f);
        if (cass_future_error_code(f) != CASS_OK) cass_die("prepare SELECT", f);
        g_select = cass_future_get_prepared(f);
        cass_future_free(f);
    }
    {
        f = cass_session_prepare(g_session,
                "INSERT INTO usertable (y_id, field0) VALUES (?, ?)");
        cass_future_wait(f);
        if (cass_future_error_code(f) != CASS_OK) cass_die("prepare INSERT", f);
        g_insert = cass_future_get_prepared(f);
        cass_future_free(f);
    }

    printf("Prepared SELECT and INSERT\n");
}

static void cass_disconnect(void) {
    cass_prepared_free(g_select);
    cass_prepared_free(g_insert);
    CassFuture *f = cass_session_close(g_session);
    cass_future_wait(f);
    cass_future_free(f);
    cass_session_free(g_session);
    cass_cluster_free(g_cluster);
}

/* ── Worker thread ──────────────────────────────────────────────────────────── */

typedef struct { int id; pthread_t tid; } Worker;

static void *worker_fn(void *arg) {
    Worker      *w    = (Worker *)arg;
    ThreadStats *ts   = &g_stats[w->id];
    char        *vbuf = g_vbufs[w->id];
    TraceRecord  rec;

    while (!g_stop) {
        /* spin briefly, then sleep to avoid busy-waiting */
        int spins = 0;
        while (!queue_pop(&g_queue, &rec)) {
            if (g_queue.done && spins > 1000) goto done;
            if (++spins > 100) sleep_ns(100000);  /* 100 µs */
        }

        uint64_t t0 = now_ns();

        if (rec.op == OP_GET) {
            /* In load mode, skip GETs entirely — we only want to populate
             * the database with SET operations. Issuing GETs during load
             * wastes time and returns empty results anyway since the keys
             * may not have been written yet. */
            if (cfg.load_mode)
                continue;

            CassStatement *s = cass_prepared_bind(g_select);
            cass_statement_bind_string(s, 0, rec.key);
            CassFuture *f = cass_session_execute(g_session, s);
            cass_future_wait(f);
            CassError e = cass_future_error_code(f);
            if (e != CASS_OK) {
                ts->get.errors++;
                if (e == CASS_ERROR_SERVER_READ_TIMEOUT) ts->get.timeouts++;
            }
            cass_future_free(f);
            cass_statement_free(s);
            tracker_record(&ts->get, (now_ns() - t0) / 1000.0);

        } else if (rec.op == OP_SET) {
            int vsz = rec.value_size;
            if (vsz <= 0) vsz = 1;
            /* vsz is guaranteed <= g_max_value_size (found during prescan)
             * so the pre-allocated buffer is always large enough — no cap needed */

            CassStatement *s = cass_prepared_bind(g_insert);
            cass_statement_bind_string(s, 0, rec.key);

            /*
             * field0 is VARCHAR.
             *
             * --least mode:
             *   vbuf[0]     = 0x00  (LEAST EC trigger byte)
             *   vbuf[1..]   = 'a'   (payload)
             *   total bound = vsz bytes
             *
             * default mode:
             *   vbuf[0..]   = 'a'
             *   total bound = vsz bytes
             */
            cass_statement_bind_string_n(s, 1, vbuf, (size_t)vsz);

            CassFuture *f = cass_session_execute(g_session, s);
            cass_future_wait(f);
            CassError e = cass_future_error_code(f);
            if (e != CASS_OK) {
                ts->set.errors++;
                if (e == CASS_ERROR_SERVER_WRITE_TIMEOUT) ts->set.timeouts++;
            }
            cass_future_free(f);
            cass_statement_free(s);
            tracker_record(&ts->set, (now_ns() - t0) / 1000.0);
        }
        /* OP_SKIP: silently ignored */
    }
done:
    return NULL;
}

/* ── Manual mode workers ─────────────────────────────────────────────────────── */

/*
 * Key format: "user%010lu"  (zero-padded 10-digit number)
 * Matches YCSB key format so it lands in the same usertable schema.
 * Example: user0000000000, user0000000001, ..., user0099999999
 */
static inline void make_key(char *buf, uint64_t idx) {
    snprintf(buf, 32, "user%010lu", (unsigned long)idx);
}

/*
 * Manual load worker.
 *
 * Threads compete for keys via an atomic counter (g_next_key).
 * Each thread claims the next available key index and writes it.
 * No queue, no pacing — pure closed-loop write until all keys done.
 *
 * Progress is naturally tracked via g_next_key.
 */
static void *manual_load_worker(void *arg) {
    Worker      *w    = (Worker *)arg;
    ThreadStats *ts   = &g_stats[w->id];
    char        *vbuf = g_vbufs[w->id];
    int          vsz  = cfg.value_size;
    char         key[32];

    while (!g_stop) {
        /* atomically claim the next key index */
        uint64_t idx = __atomic_fetch_add(&g_next_key, 1, __ATOMIC_RELAXED);
        if (idx >= cfg.num_keys)
            break;   /* all keys written */

        make_key(key, idx);

        uint64_t t0 = now_ns();

        CassStatement *s = cass_prepared_bind(g_insert);
        cass_statement_bind_string(s, 0, key);
        cass_statement_bind_string_n(s, 1, vbuf, (size_t)vsz);
        CassFuture *f = cass_session_execute(g_session, s);
        cass_future_wait(f);
        CassError e = cass_future_error_code(f);
        if (e != CASS_OK) {
            ts->set.errors++;
            if (e == CASS_ERROR_SERVER_WRITE_TIMEOUT) ts->set.timeouts++;
        }
        cass_future_free(f);
        cass_statement_free(s);
        tracker_record(&ts->set, (now_ns() - t0) / 1000.0);
    }
    return NULL;
}

/*
 * Manual benchmark worker (closed loop).
 *
 * Each thread independently picks a random key from [0, num_keys)
 * and issues a GET or SET based on read_ratio.
 *
 * Closed loop: no queue, no pacing, each thread issues the next
 * request immediately after the previous one completes.
 * Concurrency = num_threads.
 */
static void *manual_bench_worker(void *arg) {
    Worker      *w    = (Worker *)arg;
    ThreadStats *ts   = &g_stats[w->id];
    char        *vbuf = g_vbufs[w->id];
    int          vsz  = cfg.value_size;
    char         key[32];
    unsigned int seed = (unsigned int)(w->id + 1) * 2654435761u;  /* per-thread seed */

    while (!g_stop) {
        /* uniform random key from the load phase key space */
        uint64_t idx = ((uint64_t)rand_r(&seed) * (uint64_t)rand_r(&seed))
                       % cfg.num_keys;
        make_key(key, idx);

        /* decide GET or SET based on read_ratio */
        double r = (double)rand_r(&seed) / (double)RAND_MAX;

        uint64_t t0 = now_ns();

        if (r < cfg.read_ratio) {
            /* GET */
            CassStatement *s = cass_prepared_bind(g_select);
            cass_statement_bind_string(s, 0, key);
            CassFuture *f = cass_session_execute(g_session, s);
            cass_future_wait(f);
            CassError e = cass_future_error_code(f);
            if (e != CASS_OK) {
                ts->get.errors++;
                if (e == CASS_ERROR_SERVER_READ_TIMEOUT) ts->get.timeouts++;
            }
            cass_future_free(f);
            cass_statement_free(s);
            tracker_record(&ts->get, (now_ns() - t0) / 1000.0);
        } else {
            /* SET */
            CassStatement *s = cass_prepared_bind(g_insert);
            cass_statement_bind_string(s, 0, key);
            cass_statement_bind_string_n(s, 1, vbuf, (size_t)vsz);
            CassFuture *f = cass_session_execute(g_session, s);
            cass_future_wait(f);
            CassError e = cass_future_error_code(f);
            if (e != CASS_OK) {
                ts->set.errors++;
                if (e == CASS_ERROR_SERVER_WRITE_TIMEOUT) ts->set.timeouts++;
            }
            cass_future_free(f);
            cass_statement_free(s);
            tracker_record(&ts->set, (now_ns() - t0) / 1000.0);
        }
    }
    return NULL;
}

/* ── Trace parsing ───────────────────────────────────────────────────────────── */

static inline OpType parse_op(const char *s) {
    if (!strcmp(s,"get") || !strcmp(s,"gets")) return OP_GET;
    if (!strcmp(s,"set") || !strcmp(s,"add")  ||
        !strcmp(s,"replace") || !strcmp(s,"cas")) return OP_SET;
    return OP_SKIP;
}

/*
 * Line format:
 *   timestamp, key, key_size, value_size, client_id, operation[, ttl]
 *
 * We use value_size to size the synthetic VARCHAR value sent to Cassandra.
 * The TTL field from the trace is intentionally ignored when --disable-ttl
 * is set — data stays permanent so the working set grows past RAM.
 */
static int parse_line(char *line, TraceRecord *out) {
    char *p = NULL;

    char *ts  = strtok_r(line,  ",", &p); if (!ts)  return -1;
    char *key = strtok_r(NULL,  ",", &p); if (!key) return -1;
    char *ksz = strtok_r(NULL,  ",", &p); if (!ksz) return -1;
    char *vsz = strtok_r(NULL,  ",", &p); if (!vsz) return -1;
    char *cid = strtok_r(NULL,  ",", &p); if (!cid) return -1;
    char *op  = strtok_r(NULL,  ",", &p); if (!op)  return -1;

    /* trim whitespace / newline from key and op */
    while (*key == ' ') key++;
    while (*op  == ' ') op++;
    char *end = op + strlen(op) - 1;
    while (end > op && (*end=='\n'||*end=='\r'||*end==' ')) *end-- = '\0';

    out->timestamp  = (uint64_t)strtoull(ts, NULL, 10);
    out->value_size = atoi(vsz);
    out->op         = parse_op(op);
    strncpy(out->key, key, MAX_KEY_LEN - 1);
    out->key[MAX_KEY_LEN - 1] = '\0';

    return 0;
}

/* ── Trace reader (runs on main thread) ─────────────────────────────────────── */

/*
 * Pre-scan the trace file to count total lines, GET lines, and SET lines.
 * This takes a few seconds for large traces but gives accurate ETA and
 * progress percentage in the reporter.
 */
static void prescan_trace(void) {
    FILE *fp = fopen(cfg.trace_path, "r");
    if (!fp) { perror("fopen trace (prescan)"); return; }

    printf("Pre-scanning trace");
    if (cfg.load_mode && cfg.load_buffer >= 0)
        printf(" (window: first %ds + %ds buffer = %ds)",
               cfg.duration_sec, cfg.load_buffer,
               cfg.duration_sec + cfg.load_buffer);
    printf("...\n");

    uint64_t total = 0, gets = 0, sets = 0, skipped = 0;
    int      max_vsz = 0;
    uint64_t first_ts = 0;
    bool     first    = true;
    /* cutoff only applies during load with --load-buffer */
    bool     use_cutoff = (cfg.load_mode && cfg.load_buffer >= 0);
    uint64_t cutoff_ts  = 0;   /* set after first timestamp is seen */
    char line[1024];

    while (fgets(line, sizeof(line), fp)) {
        char tmp[1024];
        memcpy(tmp, line, sizeof(tmp) - 1);
        tmp[sizeof(tmp) - 1] = '\0';
        char *p = NULL;

        char *ts_tok  = strtok_r(tmp,  ",", &p);
        char *key_tok = strtok_r(NULL, ",", &p); (void)key_tok;
        char *ksz_tok = strtok_r(NULL, ",", &p); (void)ksz_tok;
        char *vsz_tok = strtok_r(NULL, ",", &p);
        char *cid_tok = strtok_r(NULL, ",", &p); (void)cid_tok;
        char *op_tok  = strtok_r(NULL, ",", &p);

        if (!ts_tok || !op_tok) { skipped++; continue; }

        uint64_t ts = (uint64_t)strtoull(ts_tok, NULL, 10);

        /* record first timestamp and compute cutoff */
        if (first) {
            first_ts   = ts;
            cutoff_ts  = first_ts + (uint64_t)cfg.duration_sec
                       + (use_cutoff ? (uint64_t)cfg.load_buffer : 0);
            first = false;
        }

        /* trace is sorted — once we exceed cutoff, we are done */
        if (use_cutoff && ts > cutoff_ts)
            break;

        /* trim op string */
        while (*op_tok == ' ') op_tok++;
        char *end = op_tok + strlen(op_tok) - 1;
        while (end > op_tok && (*end=='\n'||*end=='\r'||*end==' ')) *end-- = '\0';

        OpType t = parse_op(op_tok);
        if (t == OP_GET) {
            gets++;
        } else if (t == OP_SET) {
            sets++;
            int vsz = vsz_tok ? atoi(vsz_tok) : 0;
            if (vsz > max_vsz) max_vsz = vsz;
        } else {
            skipped++;
        }
        total++;
    }

    fclose(fp);

    g_total_lines    = total;
    g_total_gets     = gets;
    g_total_sets     = sets;
    g_max_value_size = max_vsz;

    printf("Trace stats (within scan window):\n");
    printf("  Total lines   : %lu\n",  (unsigned long)total);
    printf("  GET ops       : %lu  (%.1f%%)\n",
           (unsigned long)gets,
           total > 0 ? (double)gets / total * 100.0 : 0.0);
    printf("  SET ops       : %lu  (%.1f%%)\n",
           (unsigned long)sets,
           total > 0 ? (double)sets / total * 100.0 : 0.0);
    printf("  Max value size: %d bytes  (%.1f KB)\n",
           max_vsz, max_vsz / 1024.0);
    printf("  Skipped       : %lu\n\n", (unsigned long)skipped);
}

static void run_reader(void) {
    FILE *fp = fopen(cfg.trace_path, "r");
    if (!fp) { perror("fopen trace"); exit(1); }

    char     line[1024];
    uint64_t first_trace_ts = 0;
    uint64_t start_wall     = now_ns();
    bool     first          = true;
    uint64_t submitted = 0, skipped = 0;

    /* load-buffer cutoff: only populated when --load and --load-buffer are set.
     * Since the trace is sorted by timestamp, we break as soon as we exceed it. */
    bool     use_cutoff = (cfg.load_mode && cfg.load_buffer >= 0);
    uint64_t cutoff_ts  = 0;   /* set after first timestamp is seen */

    while (!g_stop && fgets(line, sizeof(line), fp)) {
        /* fast timestamp extraction directly from raw line —
         * identical logic to prescan, no dependency on parse_line */
        uint64_t ts = (uint64_t)strtoull(line, NULL, 10);

        /* set cutoff on first line (ts=0 lines included) */
        if (first) {
            first_trace_ts = ts;
            cutoff_ts = first_trace_ts
                      + (uint64_t)cfg.duration_sec
                      + (use_cutoff ? (uint64_t)cfg.load_buffer : 0);
            first = false;
            printf("Load cutoff: first_ts=%lu  cutoff_ts=%lu\n",
                   (unsigned long)first_trace_ts,
                   (unsigned long)cutoff_ts);
        }

        /* trace is sorted — break as soon as we exceed load window */
        if (use_cutoff && ts > cutoff_ts)
            break;

        /* honour wall-clock duration limit (benchmark phase, no cutoff) */
        if (!cfg.full_trace && cfg.load_buffer < 0 &&
            now_ns() - start_wall > (uint64_t)cfg.duration_sec * 1000000000ULL)
            break;

        char tmp[1024];
        memcpy(tmp, line, sizeof(tmp) - 1);
        tmp[sizeof(tmp) - 1] = '\0';
        TraceRecord rec;
        if (parse_line(tmp, &rec) < 0 || rec.op == OP_SKIP) {
            skipped++;
            continue;
        }

        /* pacing for benchmark phase */
        if (cfg.speed > 0.0) {
            uint64_t trace_ns = (uint64_t)(
                (double)(ts - first_trace_ts) / cfg.speed * 1e9);
            uint64_t wall_ns  = now_ns() - start_wall;
            if (trace_ns > wall_ns) sleep_ns(trace_ns - wall_ns);
        }

        while (!g_stop && !queue_push(&g_queue, &rec))
            sleep_ns(10000);

        submitted++;
    }

    fclose(fp);
    g_queue.done = 1;
    printf("\nTrace done: submitted=%lu  skipped=%lu\n",
           (unsigned long)submitted, (unsigned long)skipped);
}

/* ── Progress reporter ───────────────────────────────────────────────────────── */

static FILE    *g_csv    = NULL;
static uint64_t g_t_start;

static void do_report(void) {
    /* aggregate across all threads */
    uint64_t gc=0, sc=0, ge=0, se=0;
    double   gs=0, ss=0;
    for (int i = 0; i < cfg.num_threads; i++) {
        gc += g_stats[i].get.count;  gs += g_stats[i].get.sum_us;
        sc += g_stats[i].set.count;  ss += g_stats[i].set.sum_us;
        ge += g_stats[i].get.errors;
        se += g_stats[i].set.errors;
    }

    double elapsed = (double)(now_ns() - g_t_start) / 1e9;
    double ops_s   = elapsed > 0 ? (gc + sc) / elapsed : 0.0;
    double gmean   = gc ? gs / gc : 0.0;
    double smean   = sc ? ss / sc : 0.0;

    /*
     * Progress based on ops EXECUTED by workers, not lines read by reader.
     *
     * During --load mode: only SETs execute, so progress = sets_done / total_sets.
     * During benchmark:   both execute,   so progress = ops_done  / total_ops.
     *
     * This correctly shows progress even after the reader thread finishes,
     * since workers may still be draining a backlog of pending SETs.
     */
    uint64_t ops_done, ops_total;
    if (cfg.manual && cfg.load_mode) {
        /* manual load: progress = keys written / total keys */
        ops_done  = __atomic_load_n(&g_next_key, __ATOMIC_RELAXED);
        if (ops_done > cfg.num_keys) ops_done = cfg.num_keys;
        ops_total = cfg.num_keys;
    } else if (cfg.load_mode) {
        /* trace load: only SETs run */
        ops_done  = sc;
        ops_total = g_total_sets;
    } else {
        /* benchmark (trace or manual): GETs + SETs */
        ops_done  = gc + sc;
        ops_total = cfg.manual ? 0 : (g_total_gets + g_total_sets);
    }

    double pct = (ops_total > 0)
               ? (double)ops_done / ops_total * 100.0
               : 0.0;
    if (pct > 100.0) pct = 100.0;

    /* ETA based on current execution rate, not reader dispatch rate */
    double exec_rate = elapsed > 0 ? (double)ops_done / elapsed : 0.0;
    double eta_s     = (exec_rate > 0 && ops_done < ops_total)
                     ? (double)(ops_total - ops_done) / exec_rate
                     : 0.0;

    /*
     * Estimate data written so far.
     * SET count × average value size gives approximate bytes inserted.
     * Average value size is taken from the trace stats printed at startup.
     * We use SET ops completed (sc) × value_size from trace average.
     * For cluster46 avg value_size ≈ 928 B, cluster52 ≈ 221 B.
     * This is an approximation — actual on-disk size includes SSTable
     * overhead, compression (disabled), and Cassandra metadata (~100 B/row).
     */
    uint64_t total_sets_in_trace = g_total_sets;
    (void)total_sets_in_trace;

    char tbuf[16];
    time_t now_t = time(NULL);
    strftime(tbuf, sizeof(tbuf), "%H:%M:%S", localtime(&now_t));

    /* Format ETA nicely */
    char eta_buf[32];
    if (eta_s <= 0 || ops_total == 0)
        snprintf(eta_buf, sizeof(eta_buf), "unknown");
    else if (eta_s < 60)
        snprintf(eta_buf, sizeof(eta_buf), "%.0fs",  eta_s);
    else if (eta_s < 3600)
        snprintf(eta_buf, sizeof(eta_buf), "%.0fm%.0fs",
                 eta_s/60, fmod(eta_s,60));
    else
        snprintf(eta_buf, sizeof(eta_buf), "%.0fh%.0fm",
                 eta_s/3600, fmod(eta_s/60,60));

    printf("[%s] t=%.0fs  progress=%.1f%%  ETA=%s\n"
           "         GET n=%lu mean=%.0fµs | SET n=%lu mean=%.0fµs"
           " | err=%lu | %.0f ops/s\n",
           tbuf, elapsed, pct, eta_buf,
           (unsigned long)gc, gmean,
           (unsigned long)sc, smean,
           (unsigned long)(ge + se), ops_s);
    fflush(stdout);

    if (g_csv) {
        fprintf(g_csv, "%s,%.1f,%.1f,%s,%lu,%.1f,%lu,%.1f,%lu,%.0f\n",
                tbuf, elapsed, pct, eta_buf,
                (unsigned long)gc, gmean,
                (unsigned long)sc, smean,
                (unsigned long)(ge + se), ops_s);
        fflush(g_csv);
    }
}

static void *reporter_fn(void *arg) {
    (void)arg;
    while (!g_stop) { sleep(REPORT_INTERVAL); if (!g_stop) do_report(); }
    return NULL;
}

static void *timer_fn(void *arg) {
    (void)arg;
    sleep(cfg.duration_sec);
    g_stop = 1;
    return NULL;
}

static void sig_handler(int s) { (void)s; g_stop = 1; }

/* ── Final summary ───────────────────────────────────────────────────────────── */

static void print_summary(void) {
    /* merge all per-thread samples */
    uint64_t total_gn = 0, total_sn = 0;
    for (int i = 0; i < cfg.num_threads; i++) {
        uint64_t gn = g_stats[i].get.count;
        uint64_t sn = g_stats[i].set.count;
        total_gn += gn < MAX_SAMPLES ? gn : MAX_SAMPLES;
        total_sn += sn < MAX_SAMPLES ? sn : MAX_SAMPLES;
    }

    Tracker mg, ms;
    mg.samples = malloc((total_gn + 1) * sizeof(double));
    ms.samples = malloc((total_sn + 1) * sizeof(double));
    mg.count = ms.count = 0;
    mg.sum_us = ms.sum_us = 0;
    mg.errors = ms.errors = 0;
    mg.capacity = total_gn + 1;
    ms.capacity = total_sn + 1;

    for (int i = 0; i < cfg.num_threads; i++) {
        Tracker *g = &g_stats[i].get;
        Tracker *s = &g_stats[i].set;
        uint64_t gn = g->count < (uint64_t)MAX_SAMPLES ? g->count : MAX_SAMPLES;
        uint64_t sn = s->count < (uint64_t)MAX_SAMPLES ? s->count : MAX_SAMPLES;

        mg.count   += g->count;  mg.sum_us += g->sum_us; mg.errors += g->errors;
        ms.count   += s->count;  ms.sum_us += s->sum_us; ms.errors += s->errors;

        for (uint64_t j = 0; j < gn; j++) tracker_record(&mg, g->samples[j]);
        for (uint64_t j = 0; j < sn; j++) tracker_record(&ms, s->samples[j]);
    }

    double elapsed = (double)(now_ns() - g_t_start) / 1e9;
    uint64_t total = mg.count + ms.count;

    printf("\n══════════════════════════════════════════════════════════\n");
    printf("FINAL RESULTS  trace=%s\n", cfg.trace_path);
    printf("Mode   : %s\n", cfg.least_mode ? "LEAST (0x00 EC prefix)" : "Default Cassandra");
    printf("TTL    : %s\n", cfg.disable_ttl ? "disabled (permanent data)" : "from trace");
    printf("──────────────────────────────────────────────────────────\n");
    printf("%-6s %10s %10s %10s %10s %10s\n",
           "Op","Count","Mean µs","p50 µs","p99 µs","p999 µs");
    printf("──────────────────────────────────────────────────────────\n");

    if (mg.count)
        printf("%-6s %10lu %10.0f %10.0f %10.0f %10.0f\n", "GET",
               (unsigned long)mg.count, tracker_mean(&mg),
               tracker_pct(&mg,50), tracker_pct(&mg,99), tracker_pct(&mg,99.9));

    if (ms.count)
        printf("%-6s %10lu %10.0f %10.0f %10.0f %10.0f\n", "SET",
               (unsigned long)ms.count, tracker_mean(&ms),
               tracker_pct(&ms,50), tracker_pct(&ms,99), tracker_pct(&ms,99.9));

    printf("──────────────────────────────────────────────────────────\n");
    printf("Total ops  : %lu  (%.0f ops/s)\n",
           (unsigned long)total, elapsed > 0 ? total/elapsed : 0);
    printf("Errors GET : %lu   SET: %lu\n",
           (unsigned long)mg.errors, (unsigned long)ms.errors);
    printf("Duration   : %.1f s\n", elapsed);
    printf("══════════════════════════════════════════════════════════\n");

    free(mg.samples);
    free(ms.samples);
}

/* ── CLI ─────────────────────────────────────────────────────────────────────── */

static void usage(const char *prog) {
    printf("Usage: %s [options]\n\n", prog);
    printf("Required:\n");
    printf("  --trace    PATH   trace file (plain text, decompressed)\n\n");
    printf("Cassandra:\n");
    printf("  --host     IP     coordinator node IP (default: 10.10.1.1)\n");
    printf("  --port     N      CQL port (default: 9042)\n");
    printf("  Schema is fixed: ycsb.usertable, RF=5, DURABLE_WRITES=true\n\n");
    printf("Manual mode (no trace needed):\n");
    printf("  --manual          synthetic workload, ignores --trace\n");
    printf("  --num-keys   N    unique keys to write/read (default: 1000000)\n");
    printf("  --value-size N    fixed value size in bytes  (default: 1024)\n");
    printf("  --read-ratio F    fraction of GETs in benchmark (default: 0.5)\n\n");
    printf("Trace mode:\n");
    printf("  --threads  N      concurrent workers (default: 32)\n");
    printf("  --duration N      seconds to run (default: 3600 = 1 hour)\n");
    printf("  --speed    F      replay speed vs real-time (default: 1.0)\n");
    printf("                    1.0 = real-time, preserves original arrival pattern\n");
    printf("                    0   = max rate, ignore timestamps (load phase)\n");
    printf("  --load            skip all GETs, execute SETs only (load phase)\n");
    printf("                    must be combined with exactly one of:\n");
    printf("  --full-trace      load entire trace file (no time limit)\n");
    printf("  --load-buffer N   load SETs from first (--duration + N) seconds\n");
    printf("                    e.g. --duration 3600 --load-buffer 600\n");
    printf("                    loads SETs from timestamps 0..4200\n");
    printf("                    benchmark then replays timestamps 0..3600\n\n");
    printf("  --consistency CL  consistency level (default: ONE)\n");
    printf("                    ONE LOCAL_ONE LOCAL_QUORUM QUORUM ALL\n");
    printf("                    ONE matches YCSB default, recommended for RF=5\n\n");
    printf("LEAST / data size:\n");
    printf("  --least           prepend 0x00 to field0 (LEAST EC trigger)\n");
    printf("  --disable-ttl     ignore trace TTLs, write data permanently\n");
    printf("                    use this to push working set past 32 GB RAM\n\n");
    printf("Output:\n");
    printf("  --output   PATH   latency timeseries CSV (default: latency.csv)\n\n");
    printf("  # Manual load: write 10M keys of 1KB each\n");
    printf("  %s --manual --load --num-keys 10000000 --value-size 1024 \\\n", prog);
    printf("       --least --host 10.10.1.1 --threads 64 \\\n");
    printf("       --output /dev/null\n\n");
    printf("  # Manual benchmark: 68%% reads, closed loop, 1 hour\n");
    printf("  %s --manual --num-keys 10000000 --value-size 1024 \\\n", prog);
    printf("       --read-ratio 0.68 --least --host 10.10.1.1 \\\n");
    printf("       --threads 64 --duration 3600 --output least_manual.csv\n\n");
    printf("  # Trace load phase: SETs only, max rate, run until trace ends\n");
    printf("  %s --trace cluster50.sort --host 10.10.1.1 \\\n", prog);
    printf("       --least --disable-ttl --consistency ONE \\\n");
    printf("       --load --full-trace --speed 0 --threads 64 \\\n");
    printf("       --output /dev/null\n\n");
    printf("  # Benchmark phase: GETs + SETs, real-time, first 1 hour\n");
    printf("  %s --trace cluster50.sort --host 10.10.1.1 \\\n", prog);
    printf("       --least --disable-ttl --consistency ONE \\\n");
    printf("       --speed 1.0 --duration 3600 --threads 64 \\\n");
    printf("       --output least_c50.csv\n");
}

static void parse_args(int argc, char **argv) {
    static struct option opts[] = {
        {"trace",       required_argument, 0, 't'},
        {"host",        required_argument, 0, 'H'},
        {"port",        required_argument, 0, 'p'},
        {"threads",     required_argument, 0, 'n'},
        {"duration",    required_argument, 0, 'd'},
        {"speed",       required_argument, 0, 's'},
        {"consistency", required_argument, 0, 'c'},
        {"least",       no_argument,       0, 'L'},
        {"disable-ttl", no_argument,       0, 'X'},
        {"full-trace",  no_argument,       0, 'F'},
        {"load",        no_argument,       0, 'W'},
        {"load-buffer", required_argument, 0, 'B'},
        /* manual mode */
        {"manual",      no_argument,       0, 'M'},
        {"num-keys",    required_argument, 0, 'K'},
        {"value-size",  required_argument, 0, 'V'},
        {"read-ratio",  required_argument, 0, 'R'},
        {"output",      required_argument, 0, 'o'},
        {"help",        no_argument,       0, 'h'},
        {0,0,0,0}
    };
    int c, idx;
    while ((c = getopt_long(argc, argv, "t:H:p:n:d:s:c:LXFWo:MK:V:R:B:h",
                            opts, &idx)) != -1) {
        switch (c) {
        case 't': cfg.trace_path  = optarg;              break;
        case 'H': cfg.hosts       = optarg;              break;
        case 'p': cfg.port        = atoi(optarg);        break;
        case 'n': cfg.num_threads = atoi(optarg);        break;
        case 'd': cfg.duration_sec= atoi(optarg);        break;
        case 's': cfg.speed       = atof(optarg);        break;
        case 'c': cfg.consistency = optarg;              break;
        case 'L': cfg.least_mode  = 1;                   break;
        case 'X': cfg.disable_ttl = 1;                   break;
        case 'F': cfg.full_trace  = 1;                   break;
        case 'W': cfg.load_mode   = 1;                   break;
        case 'B': cfg.load_buffer = atoi(optarg);        break;
        case 'M': cfg.manual      = 1;                   break;
        case 'K': cfg.num_keys    = (uint64_t)atoll(optarg); break;
        case 'V': cfg.value_size  = atoi(optarg);        break;
        case 'R': cfg.read_ratio  = atof(optarg);        break;
        case 'o': cfg.output_csv  = optarg;              break;
        case 'h': usage(argv[0]); exit(0);
        default:  usage(argv[0]); exit(1);
        }
    }
    if (!cfg.manual && !cfg.trace_path) {
        fprintf(stderr, "ERROR: --trace is required (or use --manual)\n\n");
        usage(argv[0]); exit(1);
    }

    /* --load-buffer and --full-trace are mutually exclusive */
    if (cfg.load_buffer >= 0 && cfg.full_trace) {
        fprintf(stderr,
            "ERROR: --load-buffer and --full-trace cannot both be set.\n"
            "  --full-trace   : load entire trace file (no time limit)\n"
            "  --load-buffer N: load only first (duration + N) seconds\n"
            "Choose one.\n\n");
        exit(1);
    }

    /* during trace load, exactly one of --full-trace or --load-buffer must be set */
    if (!cfg.manual && cfg.load_mode) {
        if (!cfg.full_trace && cfg.load_buffer < 0) {
            fprintf(stderr,
                "ERROR: --load requires either --full-trace or --load-buffer N.\n"
                "  --full-trace      : load the entire trace file\n"
                "  --load-buffer N   : load only first (--duration + N) seconds\n"
                "                      e.g. --duration 3600 --load-buffer 600\n"
                "                      loads all SETs from the first 4200 seconds\n\n");
            exit(1);
        }
    }
}

/* ── main ────────────────────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    parse_args(argc, argv);
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    printf("══════════════════════════════════════════════════════════\n");
    printf("LEAST Trace Driver\n");
    if (cfg.manual) {
        printf("Mode     : MANUAL (synthetic workload, no trace)\n");
        printf("Phase    : %s\n", cfg.load_mode
               ? "LOAD  (write all keys)"
               : "BENCHMARK (closed loop, read/write mix)");
        printf("Keys     : %lu\n", (unsigned long)cfg.num_keys);
        printf("Val size : %d bytes\n", cfg.value_size);
        if (!cfg.load_mode)
            printf("Read ratio: %.2f  (%.0f%% GET / %.0f%% SET)\n",
                   cfg.read_ratio,
                   cfg.read_ratio * 100.0,
                   (1.0 - cfg.read_ratio) * 100.0);
    } else {
        printf("Trace    : %s\n", cfg.trace_path);
        printf("Phase    : %s\n", cfg.load_mode
               ? "LOAD  (SETs only, GETs skipped)"
               : "BENCHMARK  (GETs + SETs measured)");
        if (cfg.load_mode && cfg.load_buffer >= 0)
            printf("Load win : first %ds + %ds buffer = %ds of trace\n",
                   cfg.duration_sec, cfg.load_buffer,
                   cfg.duration_sec + cfg.load_buffer);
        if (cfg.full_trace)
            printf("Duration : until trace ends (--full-trace)\n");
        else
            printf("Duration : %ds (%.1f hours)\n",
                   cfg.duration_sec, cfg.duration_sec / 3600.0);
        printf("Speed    : %.1fx%s\n", cfg.speed,
               cfg.speed == 0.0 ? " (max rate)" :
               cfg.speed == 1.0 ? " (real-time)" : "");
        printf("TTL      : %s\n", cfg.disable_ttl
               ? "disabled" : "from trace");
    }
    printf("Host     : %s:%d\n", cfg.hosts, cfg.port);
    printf("Keyspace : ycsb.usertable  (RF=5, DURABLE_WRITES=true)\n");
    printf("Threads  : %d\n", cfg.num_threads);
    if (!cfg.manual && cfg.full_trace)
        printf("Duration : until trace ends\n");
    else if (!cfg.manual)
        printf("Duration : %ds\n", cfg.duration_sec);
    printf("EC Mode  : %s\n", cfg.least_mode
           ? "LEAST  (0x00 prefix on field0)"
           : "Default Cassandra (no prefix)");
    printf("Consist  : %s\n", cfg.consistency);
    printf("Output   : %s\n", cfg.output_csv);
    printf("══════════════════════════════════════════════════════════\n\n");

    cass_connect();

    /* In manual mode skip prescan; in trace mode prescan the file */
    if (cfg.manual) {
        g_total_sets = cfg.num_keys;   /* progress denominator for load */
        g_total_gets = 0;
        g_total_lines= cfg.num_keys;
        g_max_value_size = cfg.value_size;
        printf("Manual mode: %lu keys × %d bytes = %.1f GB\n\n",
               (unsigned long)cfg.num_keys,
               cfg.value_size,
               (double)cfg.num_keys * cfg.value_size / (1024.0*1024.0*1024.0));
    } else {
        prescan_trace();
    }

    /* Allocate per-thread resources */
    g_stats = calloc(cfg.num_threads, sizeof(ThreadStats));
    g_vbufs = malloc(cfg.num_threads * sizeof(char *));
    int buf_size = g_max_value_size + 1;
    for (int i = 0; i < cfg.num_threads; i++) {
        tracker_init(&g_stats[i].get);
        tracker_init(&g_stats[i].set);
        g_vbufs[i] = malloc(buf_size);
        if (!g_vbufs[i]) { perror("malloc vbuf"); exit(1); }
        if (cfg.least_mode) {
            g_vbufs[i][0] = '\x00';
            memset(g_vbufs[i] + 1, 'a', buf_size - 2);
        } else {
            memset(g_vbufs[i], 'a', buf_size - 1);
        }
        g_vbufs[i][buf_size - 1] = '\0';
    }
    printf("Value buffer: %d bytes/thread × %d threads = %.1f MB\n\n",
           buf_size, cfg.num_threads,
           (double)buf_size * cfg.num_threads / (1024.0 * 1024.0));

    if (!cfg.manual)
        queue_init(&g_queue, QUEUE_CAPACITY);

    /* CSV header */
    if (cfg.output_csv && strcmp(cfg.output_csv, "/dev/null") != 0) {
        g_csv = fopen(cfg.output_csv, "w");
        if (!g_csv) perror("fopen output");
        else fprintf(g_csv,
                     "time,elapsed_s,progress_pct,eta,"
                     "get_count,get_mean_us,"
                     "set_count,set_mean_us,"
                     "errors,ops_per_sec\n");
    }

    g_t_start = now_ns();

    /* Start workers — different function for manual vs trace mode */
    Worker *workers = malloc(cfg.num_threads * sizeof(Worker));
    void *(*wfn)(void *) = cfg.manual
        ? (cfg.load_mode ? manual_load_worker : manual_bench_worker)
        : worker_fn;
    for (int i = 0; i < cfg.num_threads; i++) {
        workers[i].id = i;
        pthread_create(&workers[i].tid, NULL, wfn, &workers[i]);
    }

    /* Start reporter and duration timer */
    pthread_t rep_tid, timer_tid;
    pthread_create(&rep_tid, NULL, reporter_fn, NULL);
    /* Duration timer: not needed for manual load (workers self-terminate
     * when all keys are written). Used for benchmark phases only. */
    if (!(cfg.manual && cfg.load_mode))
        pthread_create(&timer_tid, NULL, timer_fn, NULL);

    if (cfg.manual) {
        /* Manual mode: workers are self-contained, just wait for them */
        for (int i = 0; i < cfg.num_threads; i++)
            pthread_join(workers[i].tid, NULL);
    } else {
        /* Trace mode: reader runs on main thread, feeds the ring queue */
        run_reader();
        /* Wait for queue to drain */
        while (!g_stop) {
            if (__atomic_load_n(&g_queue.head, __ATOMIC_RELAXED) ==
                __atomic_load_n(&g_queue.tail, __ATOMIC_RELAXED)) break;
            sleep_ns(1000000);
        }
        g_stop = 1;
        for (int i = 0; i < cfg.num_threads; i++)
            pthread_join(workers[i].tid, NULL);
    }

    pthread_join(rep_tid, NULL);
    if (!(cfg.manual && cfg.load_mode))
        pthread_cancel(timer_tid);

    print_summary();

    if (g_csv) fclose(g_csv);
    for (int i = 0; i < cfg.num_threads; i++) {
        tracker_free(&g_stats[i].get);
        tracker_free(&g_stats[i].set);
        free(g_vbufs[i]);
    }
    free(g_stats); free(g_vbufs); free(workers);
    cass_disconnect();
    return 0;
}
