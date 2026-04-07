/*
 * trace_driver.c
 *
 * Twitter cache trace replay driver for Cassandra / LEAST.
 *
 * Reads a Twitter Twemcache trace (plain text CSV), maps:
 *   GET  →  SELECT field0 FROM ycsb.usertable WHERE y_id = ?
 *   SET  →  INSERT INTO ycsb.usertable (y_id, field0) VALUES (?, ?)
 *
 * Build:
 *   gcc -O2 -o trace_driver trace_driver.c -lcassandra -lpthread -lm
 *
 * Typical workflow (pre-filtered trace):
 *
 *   # 0. Filter trace once
 *   python3 filter_trace.py cluster46.sort cluster46.filtered 3600 600 100
 *
 *   # 1. Load phase — write all SETs in filtered file
 *   ./trace_driver --trace cluster46.filtered --host 10.10.1.1 \
 *                  --least --disable-ttl --consistency ONE \
 *                  --load --full-trace --speed 0 --threads 64 \
 *                  --output /dev/null
 *
 *   # 2. Benchmark phase — replay first 1 hour at real-time speed
 *   ./trace_driver --trace cluster46.filtered --host 10.10.1.1 \
 *                  --least --disable-ttl --consistency ONE \
 *                  --speed 1.0 --duration 3600 --threads 64 \
 *                  --output results/least_c46.csv
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

/* ── Constants ───────────────────────────────────────────────────────────────── */

#define MAX_KEY_LEN     256
#define QUEUE_CAPACITY  8192       /* ring queue size, must be power of 2 */
#define REPORT_INTERVAL 10         /* seconds between progress reports */
#define MAX_SAMPLES     2000000    /* max latency samples per thread */

/* ── Types ───────────────────────────────────────────────────────────────────── */

typedef enum { OP_GET = 0, OP_SET = 1, OP_SKIP = 2 } OpType;

typedef struct {
    uint64_t timestamp;
    char     key[MAX_KEY_LEN];
    int      value_size;
    OpType   op;
} TraceRecord;

typedef struct {
    double   *samples;
    uint64_t  count;       /* total ops recorded (may exceed MAX_SAMPLES) */
    uint64_t  capacity;    /* = MAX_SAMPLES */
    double    sum_us;
    uint64_t  errors;
    uint64_t  timeouts;
} Tracker;

typedef struct { Tracker get; Tracker set; } ThreadStats;
typedef struct { int id; pthread_t tid; } Worker;

/* ── Ring queue (SPMC: one producer reader thread, N consumer workers) ────────── */

typedef struct {
    TraceRecord       *buf;
    volatile uint64_t  head;   /* producer writes here */
    volatile uint64_t  tail;   /* consumers read from here */
    uint64_t           mask;   /* capacity - 1 */
    volatile int       done;   /* set when producer is finished */
    pthread_mutex_t    pop_mu; /* guards tail — makes pop thread-safe */
} RingQueue;

/* ── Config ──────────────────────────────────────────────────────────────────── */

typedef struct {
    /* connection */
    const char *hosts;
    int         port;
    const char *consistency;

    /* trace mode */
    const char *trace_path;
    int         load_mode;      /* 1 = SETs only, skip GETs */
    int         full_trace;     /* 1 = read entire file, ignore duration */
    int         load_buffer;    /* seconds beyond duration for load window (-1 = not set) */
    int         disable_ttl;
    double      speed;          /* 0 = max rate, 1.0 = real-time */

    /* manual mode */
    int         manual;
    uint64_t    num_keys;
    int         value_size;
    double      read_ratio;

    /* common */
    int         num_threads;
    int         duration_sec;
    const char *output_csv;
    int         least_mode;
} Config;

static Config cfg = {
    .hosts        = "10.10.1.1",
    .port         = 9042,
    .consistency  = "ONE",
    .trace_path   = NULL,
    .load_mode    = 0,
    .full_trace   = 0,
    .load_buffer  = -1,
    .disable_ttl  = 0,
    .speed        = 1.0,
    .manual       = 0,
    .num_keys     = 1000000,
    .value_size   = 1024,
    .read_ratio   = 0.5,
    .num_threads  = 32,
    .duration_sec = 3600,
    .output_csv   = "latency.csv",
    .least_mode   = 0,
};

/* ── Globals ─────────────────────────────────────────────────────────────────── */

static volatile int  g_stop       = 0;
static RingQueue     g_queue;
static CassCluster  *g_cluster    = NULL;
static CassSession  *g_session    = NULL;
static const CassPrepared *g_select = NULL;
static const CassPrepared *g_insert = NULL;
static ThreadStats  *g_stats      = NULL;
static char        **g_vbufs      = NULL;
static int           g_max_value_size = 0;  /* set by prescan or manual mode */

/* prescan results — used for progress tracking */
static volatile uint64_t g_total_lines = 0;  /* ALL lines in scan window */
static volatile uint64_t g_total_sets  = 0;
static volatile uint64_t g_total_gets  = 0;

/* manual load progress */
static volatile uint64_t g_next_key = 0;
static volatile uint64_t g_popped   = 0;   /* total records popped from queue */

/* timing */
static uint64_t g_t_start = 0;
static FILE    *g_csv     = NULL;

/* ── Timing helpers ──────────────────────────────────────────────────────────── */

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

/* ── Tracker ─────────────────────────────────────────────────────────────────── */

static void tracker_init(Tracker *t) {
    t->samples  = malloc(MAX_SAMPLES * sizeof(double));
    t->count    = 0;
    t->capacity = MAX_SAMPLES;
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
        /* reservoir sampling — keep a representative subset */
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

/* ── Ring queue ──────────────────────────────────────────────────────────────── */

static void queue_init(RingQueue *q, uint64_t cap) {
    q->buf  = malloc(cap * sizeof(TraceRecord));
    q->head = q->tail = 0;
    q->mask = cap - 1;
    q->done = 0;
    pthread_mutex_init(&q->pop_mu, NULL);
    if (!q->buf) { perror("malloc queue"); exit(1); }
}

static inline bool queue_push(RingQueue *q, const TraceRecord *r) {
    uint64_t head = __atomic_load_n(&q->head, __ATOMIC_RELAXED);
    uint64_t tail = __atomic_load_n(&q->tail, __ATOMIC_ACQUIRE);
    if (head - tail >= (uint64_t)(q->mask + 1)) return false;  /* full */
    q->buf[head & q->mask] = *r;
    __atomic_store_n(&q->head, head + 1, __ATOMIC_RELEASE);
    return true;
}

/*
 * Thread-safe pop for multiple consumers.
 *
 * We use a mutex rather than CAS because CAS-before-read has a race:
 * after CAS increments tail, the producer can push 8192 more items and
 * overwrite the slot before the winning thread reads it.
 *
 * The mutex is held for ~1µs. Workers then spend 5–50ms in Cassandra,
 * so contention is negligible.
 */
static inline bool queue_pop(RingQueue *q, TraceRecord *out) {
    pthread_mutex_lock(&q->pop_mu);
    uint64_t tail = q->tail;
    uint64_t head = __atomic_load_n(&q->head, __ATOMIC_ACQUIRE);
    if (tail == head) {
        pthread_mutex_unlock(&q->pop_mu);
        return false;
    }
    *out = q->buf[tail & q->mask];
    /* RELEASE store so the exit-condition's ACQUIRE load sees updated tail */
    __atomic_store_n(&q->tail, tail + 1, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&q->pop_mu);
    return true;
}

/* ── Cassandra ───────────────────────────────────────────────────────────────── */

static void cass_die(const char *ctx, CassFuture *f) {
    const char *msg; size_t len;
    cass_future_error_message(f, &msg, &len);
    fprintf(stderr, "ERROR [%s]: %.*s\n", ctx, (int)len, msg);
    cass_future_free(f);
    exit(1);
}

static CassConsistency parse_consistency(const char *s) {
    if (!strcasecmp(s, "ONE"))          return CASS_CONSISTENCY_ONE;
    if (!strcasecmp(s, "TWO"))          return CASS_CONSISTENCY_TWO;
    if (!strcasecmp(s, "THREE"))        return CASS_CONSISTENCY_THREE;
    if (!strcasecmp(s, "QUORUM"))       return CASS_CONSISTENCY_QUORUM;
    if (!strcasecmp(s, "ALL"))          return CASS_CONSISTENCY_ALL;
    if (!strcasecmp(s, "LOCAL_QUORUM")) return CASS_CONSISTENCY_LOCAL_QUORUM;
    if (!strcasecmp(s, "LOCAL_ONE"))    return CASS_CONSISTENCY_LOCAL_ONE;
    fprintf(stderr, "Unknown consistency: %s\n"
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
    cass_cluster_set_whitelist_filtering(g_cluster, cfg.hosts);

    CassFuture *f = cass_session_connect(g_session, g_cluster);
    cass_future_wait(f);
    if (cass_future_error_code(f) != CASS_OK) cass_die("connect", f);
    cass_future_free(f);
    printf("Connected to Cassandra cluster!\n");

    /* CREATE KEYSPACE — exact schema from original program */
    {
        const char *q =
            "CREATE KEYSPACE IF NOT EXISTS ycsb "
            "WITH replication = {"
            "  'class': 'SimpleStrategy', 'replication_factor': '5'"
            "} AND DURABLE_WRITES = true;";
        f = cass_session_execute(g_session, cass_statement_new(q, 0));
        cass_future_wait(f);
        if (cass_future_error_code(f) != CASS_OK) {
            const char *msg; size_t len;
            cass_future_error_message(f, &msg, &len);
            fprintf(stderr, "Failed to create keyspace: %.*s\n", (int)len, msg);
        }
        cass_future_free(f);
        fprintf(stderr, "Create keyspace: Success, waiting 5s to propagate\n");
        sleep(5);
    }

    /* CREATE TABLE — exact schema from original program */
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
        }
        cass_future_free(f);
        fprintf(stderr, "Create table: Success, waiting 10s to propagate\n");
        sleep(10);
    }

    /* USE ycsb */
    f = cass_session_execute(g_session, cass_statement_new("USE ycsb", 0));
    cass_future_wait(f);
    if (cass_future_error_code(f) != CASS_OK) cass_die("USE ycsb", f);
    cass_future_free(f);

    /* PREPARE SELECT */
    f = cass_session_prepare(g_session,
            "SELECT field0 FROM usertable WHERE y_id = ?");
    cass_future_wait(f);
    if (cass_future_error_code(f) != CASS_OK) cass_die("prepare SELECT", f);
    g_select = cass_future_get_prepared(f);
    cass_future_free(f);

    /* PREPARE INSERT */
    f = cass_session_prepare(g_session,
            "INSERT INTO usertable (y_id, field0) VALUES (?, ?)");
    cass_future_wait(f);
    if (cass_future_error_code(f) != CASS_OK) cass_die("prepare INSERT", f);
    g_insert = cass_future_get_prepared(f);
    cass_future_free(f);

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

/* ── Trace parsing ───────────────────────────────────────────────────────────── */

static inline OpType parse_op(const char *s) {
    if (!strcmp(s,"get") || !strcmp(s,"gets")) return OP_GET;
    if (!strcmp(s,"set") || !strcmp(s,"add") ||
        !strcmp(s,"replace") || !strcmp(s,"cas")) return OP_SET;
    return OP_SKIP;
}

/*
 * Parse one CSV line into a TraceRecord.
 * Format: timestamp, key, key_size, value_size, client_id, operation[, ttl]
 * Returns 0 on success, -1 on malformed line.
 */
static int parse_line(char *line, TraceRecord *out) {
    char *p = NULL;
    char *ts  = strtok_r(line, ",", &p); if (!ts)  return -1;
    char *key = strtok_r(NULL, ",", &p); if (!key) return -1;
    char *ksz = strtok_r(NULL, ",", &p); if (!ksz) return -1;
    char *vsz = strtok_r(NULL, ",", &p); if (!vsz) return -1;
    char *cid = strtok_r(NULL, ",", &p); if (!cid) return -1;
    char *op  = strtok_r(NULL, ",", &p); if (!op)  return -1;

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

/* ── Prescan ─────────────────────────────────────────────────────────────────── */

/*
 * Scan the trace file to count lines and find max value_size.
 *
 * Stopping logic:
 *   --full-trace        → read entire file, no timestamp cutoff
 *   --load-buffer N     → stop when timestamp > duration + N
 *   benchmark phase     → stop when timestamp > duration
 *
 * g_total_lines counts EVERY line fgets returns (including malformed),
 * matching exactly how run_reader counts lines_read. This ensures the
 * line-count stopping mechanism in run_reader is accurate.
 */
static void prescan_trace(void) {
    FILE *fp = fopen(cfg.trace_path, "r");
    if (!fp) { perror("fopen trace (prescan)"); exit(1); }

    /*
     * Cutoff timestamp:
     *   --full-trace            → UINT64_MAX (no cutoff)
     *   --load + --load-buffer  → duration + load_buffer
     *   benchmark               → duration
     */
    uint64_t cutoff;
    if (cfg.full_trace) {
        cutoff = UINT64_MAX;
        printf("Pre-scanning trace (entire file)...\n");
    } else if (cfg.load_mode && cfg.load_buffer >= 0) {
        cutoff = (uint64_t)cfg.duration_sec + (uint64_t)cfg.load_buffer;
        printf("Pre-scanning trace (window: 0..%lus = %ds + %ds buffer)...\n",
               (unsigned long)cutoff, cfg.duration_sec, cfg.load_buffer);
    } else {
        cutoff = (uint64_t)cfg.duration_sec;
        printf("Pre-scanning trace (window: 0..%ds)...\n", cfg.duration_sec);
    }

    uint64_t total = 0, gets = 0, sets = 0, skipped = 0;
    int      max_vsz = 0;
    uint64_t first_ts = UINT64_MAX;
    char     line[1024];

    while (fgets(line, sizeof(line), fp)) {
        total++;   /* count every line — matches run_reader's lines_read */

        /* fast timestamp parse from raw line */
        uint64_t ts = (uint64_t)strtoull(line, NULL, 10);

        /* record first timestamp (used to make cutoff relative) */
        if (first_ts == UINT64_MAX) first_ts = ts;

        /* stop once we exceed the cutoff window */
        if (cutoff != UINT64_MAX && (ts - first_ts) > cutoff)
            break;

        /* parse fields for op type and value_size */
        char tmp[1024];
        memcpy(tmp, line, sizeof(tmp) - 1);
        tmp[sizeof(tmp) - 1] = '\0';
        char *p = NULL;
        strtok_r(tmp,  ",", &p);  /* timestamp  */
        strtok_r(NULL, ",", &p);  /* key        */
        strtok_r(NULL, ",", &p);  /* key_size   */
        char *vsz_tok = strtok_r(NULL, ",", &p);  /* value_size */
        strtok_r(NULL, ",", &p);  /* client_id  */
        char *op_tok  = strtok_r(NULL, ",", &p);  /* operation  */

        if (!op_tok) { skipped++; continue; }

        /* trim op */
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
    }

    fclose(fp);

    g_total_lines    = total;
    g_total_gets     = gets;
    g_total_sets     = sets;
    g_max_value_size = max_vsz;

    printf("Trace stats:\n");
    printf("  Total lines   : %lu\n",  (unsigned long)total);
    printf("  GET ops       : %lu  (%.1f%%)\n",
           (unsigned long)gets,
           total > 0 ? (double)gets / total * 100.0 : 0.0);
    printf("  SET ops       : %lu  (%.1f%%)\n",
           (unsigned long)sets,
           total > 0 ? (double)sets / total * 100.0 : 0.0);
    printf("  Max value size: %d bytes  (%.1f KB)\n",
           max_vsz, max_vsz / 1024.0);
    printf("  Skipped/other : %lu\n\n", (unsigned long)skipped);
}

/* ── Trace reader (main thread) ──────────────────────────────────────────────── */

static void run_reader(void) {
    FILE *fp = fopen(cfg.trace_path, "r");
    if (!fp) { perror("fopen trace"); exit(1); }

    /*
     * Use the same cutoff as prescan.
     * Additionally use g_total_lines as a hard line-count limit —
     * this guarantees run_reader processes exactly the same lines
     * prescan counted, regardless of timestamp rounding.
     */
    uint64_t cutoff;
    if (cfg.full_trace) {
        cutoff = UINT64_MAX;
    } else if (cfg.load_mode && cfg.load_buffer >= 0) {
        cutoff = (uint64_t)cfg.duration_sec + (uint64_t)cfg.load_buffer;
    } else {
        cutoff = (uint64_t)cfg.duration_sec;
    }

    printf("Reader: processing up to %lu lines (prescan count)\n",
           (unsigned long)g_total_lines);

    char     line[1024];
    uint64_t first_trace_ts = UINT64_MAX;
    uint64_t start_wall     = now_ns();
    uint64_t submitted      = 0;
    uint64_t submitted_sets = 0;
    uint64_t submitted_gets = 0;
    uint64_t skipped        = 0;
    uint64_t lines_read     = 0;

    while (!g_stop && fgets(line, sizeof(line), fp)) {

        /* hard line-count limit — stops at exactly the same point as prescan */
        if (lines_read >= g_total_lines) break;
        lines_read++;

        uint64_t ts = (uint64_t)strtoull(line, NULL, 10);
        if (first_trace_ts == UINT64_MAX) first_trace_ts = ts;

        /* timestamp cutoff — belt-and-suspenders with the line count limit */
        if (cutoff != UINT64_MAX && (ts - first_trace_ts) > cutoff) break;

        /* wall-clock duration limit for benchmark phase */
        if (!cfg.load_mode && !cfg.full_trace &&
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

        /* real-time pacing for benchmark phase */
        if (cfg.speed > 0.0) {
            uint64_t trace_ns = (uint64_t)(
                (double)(ts - first_trace_ts) / cfg.speed * 1e9);
            uint64_t wall_ns  = now_ns() - start_wall;
            if (trace_ns > wall_ns) sleep_ns(trace_ns - wall_ns);
        }

        while (!g_stop && !queue_push(&g_queue, &rec))
            sleep_ns(10000);

        submitted++;
        if (rec.op == OP_SET) submitted_sets++;
        else if (rec.op == OP_GET) submitted_gets++;
    }

    fclose(fp);
    g_queue.done = 1;
    printf("\nTrace done: lines_read=%lu  submitted=%lu"
           "  sets=%lu  gets=%lu  skipped=%lu\n",
           (unsigned long)lines_read,
           (unsigned long)submitted,
           (unsigned long)submitted_sets,
           (unsigned long)submitted_gets,
           (unsigned long)skipped);
}

/* ── Trace worker thread ─────────────────────────────────────────────────────── */

static void *worker_fn(void *arg) {
    Worker      *w    = (Worker *)arg;
    ThreadStats *ts   = &g_stats[w->id];
    char        *vbuf = g_vbufs[w->id];
    TraceRecord  rec;

    while (!g_stop) {
        int spins = 0;
        while (!queue_pop(&g_queue, &rec)) {
            if (g_queue.done) {
                /* Check under the mutex so we see the true tail value —
                 * no stale reads that could cause premature exit */
                pthread_mutex_lock(&g_queue.pop_mu);
                bool empty = (g_queue.tail ==
                    (uint64_t)__atomic_load_n(&g_queue.head, __ATOMIC_ACQUIRE));
                pthread_mutex_unlock(&g_queue.pop_mu);
                if (empty) goto done;
            }
            if (++spins > 100) sleep_ns(100000);
        }
        __atomic_fetch_add(&g_popped, 1, __ATOMIC_RELAXED);

        uint64_t t0 = now_ns();

        if (rec.op == OP_GET) {
            if (cfg.load_mode) continue;   /* skip GETs during load */

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
            /* vsz guaranteed <= g_max_value_size from prescan — no cap needed */

            CassStatement *s = cass_prepared_bind(g_insert);
            cass_statement_bind_string(s, 0, rec.key);
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
done:
    return NULL;
}

/* ── Manual mode workers ─────────────────────────────────────────────────────── */

static inline void make_key(char *buf, uint64_t idx) {
    snprintf(buf, 32, "user%010lu", (unsigned long)idx);
}

static void *manual_load_worker(void *arg) {
    Worker      *w    = (Worker *)arg;
    ThreadStats *ts   = &g_stats[w->id];
    char        *vbuf = g_vbufs[w->id];
    int          vsz  = cfg.value_size;
    char         key[32];

    while (!g_stop) {
        uint64_t idx = __atomic_fetch_add(&g_next_key, 1, __ATOMIC_RELAXED);
        if (idx >= cfg.num_keys) break;

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

static void *manual_bench_worker(void *arg) {
    Worker      *w    = (Worker *)arg;
    ThreadStats *ts   = &g_stats[w->id];
    char        *vbuf = g_vbufs[w->id];
    int          vsz  = cfg.value_size;
    char         key[32];
    unsigned int seed = (unsigned int)(w->id + 1) * 2654435761u;

    while (!g_stop) {
        uint64_t idx = ((uint64_t)rand_r(&seed) * (uint64_t)rand_r(&seed))
                       % cfg.num_keys;
        make_key(key, idx);
        double r = (double)rand_r(&seed) / (double)RAND_MAX;
        uint64_t t0 = now_ns();

        if (r < cfg.read_ratio) {
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

/* ── Progress reporter ───────────────────────────────────────────────────────── */

static void do_report(void) {
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

    /* progress based on worker-executed ops, not reader dispatch */
    uint64_t ops_done, ops_total;
    if (cfg.manual && cfg.load_mode) {
        ops_done  = __atomic_load_n(&g_next_key, __ATOMIC_RELAXED);
        if (ops_done > cfg.num_keys) ops_done = cfg.num_keys;
        ops_total = cfg.num_keys;
    } else if (cfg.load_mode) {
        ops_done  = sc;
        ops_total = g_total_sets;
    } else {
        ops_done  = gc + sc;
        ops_total = cfg.manual ? 0 : (g_total_gets + g_total_sets);
    }

    double pct = (ops_total > 0)
               ? fmin((double)ops_done / ops_total * 100.0, 100.0)
               : 0.0;
    double exec_rate = elapsed > 0 ? (double)ops_done / elapsed : 0.0;
    double eta_s     = (exec_rate > 0 && ops_done < ops_total)
                     ? (double)(ops_total - ops_done) / exec_rate : 0.0;

    char eta_buf[32];
    if (ops_total == 0 || eta_s <= 0)
        snprintf(eta_buf, sizeof(eta_buf), "unknown");
    else if (eta_s < 60)
        snprintf(eta_buf, sizeof(eta_buf), "%.0fs", eta_s);
    else if (eta_s < 3600)
        snprintf(eta_buf, sizeof(eta_buf), "%.0fm%.0fs", eta_s/60, fmod(eta_s,60));
    else
        snprintf(eta_buf, sizeof(eta_buf), "%.0fh%.0fm", eta_s/3600, fmod(eta_s/60,60));

    char tbuf[16];
    time_t now_t = time(NULL);
    strftime(tbuf, sizeof(tbuf), "%H:%M:%S", localtime(&now_t));

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

/*
 * Merge per-thread trackers and print p50/p99/p999.
 *
 * IMPORTANT: use memcpy to copy samples, NOT tracker_record.
 * tracker_record increments t->count internally — calling it during
 * the merge would double-count every op in the summary.
 */
static void print_summary(void) {
    /* pass 1: accumulate true counts and sample sizes */
    uint64_t true_gc = 0, true_sc = 0;
    uint64_t samp_gn = 0, samp_sn = 0;
    double   sum_gus = 0, sum_sus = 0;
    uint64_t err_g   = 0, err_s   = 0;

    for (int i = 0; i < cfg.num_threads; i++) {
        Tracker *g = &g_stats[i].get;
        Tracker *s = &g_stats[i].set;
        true_gc += g->count;  sum_gus += g->sum_us;  err_g += g->errors;
        true_sc += s->count;  sum_sus += s->sum_us;  err_s += s->errors;
        samp_gn += g->count < (uint64_t)MAX_SAMPLES ? g->count : MAX_SAMPLES;
        samp_sn += s->count < (uint64_t)MAX_SAMPLES ? s->count : MAX_SAMPLES;
    }

    /* pass 2: copy samples into flat arrays */
    double *gsamp = malloc((samp_gn + 1) * sizeof(double));
    double *ssamp = malloc((samp_sn + 1) * sizeof(double));
    uint64_t gi = 0, si = 0;
    for (int i = 0; i < cfg.num_threads; i++) {
        Tracker *g = &g_stats[i].get;
        Tracker *s = &g_stats[i].set;
        uint64_t gn = g->count < (uint64_t)MAX_SAMPLES ? g->count : MAX_SAMPLES;
        uint64_t sn = s->count < (uint64_t)MAX_SAMPLES ? s->count : MAX_SAMPLES;
        memcpy(gsamp + gi, g->samples, gn * sizeof(double)); gi += gn;
        memcpy(ssamp + si, s->samples, sn * sizeof(double)); si += sn;
    }

    qsort(gsamp, samp_gn, sizeof(double), cmp_double);
    qsort(ssamp, samp_sn, sizeof(double), cmp_double);

    #define PCT(arr, n, p) \
        ((n) > 0 ? (arr)[((uint64_t)((n)*(p)/100.0) < (n)) \
                        ? (uint64_t)((n)*(p)/100.0) : (n)-1] : 0.0)

    double elapsed = (double)(now_ns() - g_t_start) / 1e9;

    printf("\n══════════════════════════════════════════════════════════\n");
    printf("FINAL RESULTS  trace=%s\n",
           cfg.manual ? "(manual mode)" : cfg.trace_path);
    printf("Mode   : %s\n", cfg.least_mode
           ? "LEAST (0x00 EC prefix)" : "Default Cassandra");
    printf("TTL    : %s\n", cfg.disable_ttl
           ? "disabled (permanent data)" : "from trace");
    printf("──────────────────────────────────────────────────────────\n");
    printf("%-6s %10s %10s %10s %10s %10s\n",
           "Op", "Count", "Mean µs", "p50 µs", "p99 µs", "p999 µs");
    printf("──────────────────────────────────────────────────────────\n");

    if (true_gc)
        printf("%-6s %10lu %10.0f %10.0f %10.0f %10.0f\n", "GET",
               (unsigned long)true_gc, sum_gus / true_gc,
               PCT(gsamp, samp_gn, 50.0),
               PCT(gsamp, samp_gn, 99.0),
               PCT(gsamp, samp_gn, 99.9));

    if (true_sc)
        printf("%-6s %10lu %10.0f %10.0f %10.0f %10.0f\n", "SET",
               (unsigned long)true_sc, sum_sus / true_sc,
               PCT(ssamp, samp_sn, 50.0),
               PCT(ssamp, samp_sn, 99.0),
               PCT(ssamp, samp_sn, 99.9));

    printf("──────────────────────────────────────────────────────────\n");
    printf("Total ops  : %lu  (%.0f ops/s)\n",
           (unsigned long)(true_gc + true_sc),
           elapsed > 0 ? (true_gc + true_sc) / elapsed : 0);
    printf("Errors GET : %lu   SET: %lu\n",
           (unsigned long)err_g, (unsigned long)err_s);
    printf("Duration   : %.1f s\n", elapsed);
    printf("══════════════════════════════════════════════════════════\n");

    #undef PCT
    free(gsamp);
    free(ssamp);
}

/* ── CLI ─────────────────────────────────────────────────────────────────────── */

static void usage(const char *prog) {
    printf("Usage: %s [options]\n\n", prog);
    printf("Cassandra:\n");
    printf("  --host        IP    coordinator node (default: 10.10.1.1)\n");
    printf("  --port        N     CQL port (default: 9042)\n");
    printf("  --consistency CL    ONE|LOCAL_ONE|LOCAL_QUORUM|QUORUM|ALL (default: ONE)\n\n");
    printf("Trace mode:\n");
    printf("  --trace       PATH  trace file (plain text)\n");
    printf("  --load              SETs only, skip GETs\n");
    printf("                      requires exactly one of:\n");
    printf("  --full-trace        read entire file (use with filtered trace)\n");
    printf("  --load-buffer N     load SETs from 0..(duration+N)s\n");
    printf("  --disable-ttl       store data permanently (no expiry)\n");
    printf("  --speed       F     1.0=real-time  0=max rate (default: 1.0)\n\n");
    printf("Manual mode:\n");
    printf("  --manual            synthetic workload, no trace needed\n");
    printf("  --load              write all --num-keys keys\n");
    printf("  --num-keys    N     unique keys (default: 1000000)\n");
    printf("  --value-size  N     value bytes (default: 1024)\n");
    printf("  --read-ratio  F     GET fraction in benchmark (default: 0.5)\n\n");
    printf("Common:\n");
    printf("  --threads     N     worker threads (default: 32)\n");
    printf("  --duration    N     seconds (default: 3600)\n");
    printf("  --least             prepend 0x00 to field0 (LEAST EC trigger)\n");
    printf("  --output      PATH  CSV output (use /dev/null for load)\n\n");
    printf("Examples:\n");
    printf("  # Trace load\n");
    printf("  %s --trace cluster46.filtered --host 10.10.1.1 \\\n", prog);
    printf("       --least --disable-ttl --consistency ONE \\\n");
    printf("       --load --full-trace --speed 0 --threads 64 --output /dev/null\n\n");
    printf("  # Trace benchmark\n");
    printf("  %s --trace cluster46.filtered --host 10.10.1.1 \\\n", prog);
    printf("       --least --disable-ttl --consistency ONE \\\n");
    printf("       --speed 1.0 --duration 3600 --threads 64 --output least.csv\n\n");
    printf("  # Manual load\n");
    printf("  %s --manual --load --num-keys 10000000 --value-size 1024 \\\n", prog);
    printf("       --least --host 10.10.1.1 --threads 64 --output /dev/null\n\n");
    printf("  # Manual benchmark\n");
    printf("  %s --manual --num-keys 10000000 --value-size 1024 \\\n", prog);
    printf("       --read-ratio 0.68 --least --host 10.10.1.1 \\\n");
    printf("       --threads 64 --duration 3600 --output least_manual.csv\n");
}

static void parse_args(int argc, char **argv) {
    static struct option opts[] = {
        {"host",        required_argument, 0, 'H'},
        {"port",        required_argument, 0, 'p'},
        {"consistency", required_argument, 0, 'c'},
        {"trace",       required_argument, 0, 't'},
        {"load",        no_argument,       0, 'W'},
        {"full-trace",  no_argument,       0, 'F'},
        {"load-buffer", required_argument, 0, 'B'},
        {"disable-ttl", no_argument,       0, 'X'},
        {"speed",       required_argument, 0, 's'},
        {"manual",      no_argument,       0, 'M'},
        {"num-keys",    required_argument, 0, 'K'},
        {"value-size",  required_argument, 0, 'V'},
        {"read-ratio",  required_argument, 0, 'R'},
        {"threads",     required_argument, 0, 'n'},
        {"duration",    required_argument, 0, 'd'},
        {"least",       no_argument,       0, 'L'},
        {"output",      required_argument, 0, 'o'},
        {"help",        no_argument,       0, 'h'},
        {0,0,0,0}
    };
    int c, idx;
    while ((c = getopt_long(argc, argv,
                            "H:p:c:t:WFB:Xs:MK:V:R:n:d:Lo:h",
                            opts, &idx)) != -1) {
        switch (c) {
        case 'H': cfg.hosts       = optarg;              break;
        case 'p': cfg.port        = atoi(optarg);        break;
        case 'c': cfg.consistency = optarg;              break;
        case 't': cfg.trace_path  = optarg;              break;
        case 'W': cfg.load_mode   = 1;                   break;
        case 'F': cfg.full_trace  = 1;                   break;
        case 'B': cfg.load_buffer = atoi(optarg);        break;
        case 'X': cfg.disable_ttl = 1;                   break;
        case 's': cfg.speed       = atof(optarg);        break;
        case 'M': cfg.manual      = 1;                   break;
        case 'K': cfg.num_keys    = (uint64_t)atoll(optarg); break;
        case 'V': cfg.value_size  = atoi(optarg);        break;
        case 'R': cfg.read_ratio  = atof(optarg);        break;
        case 'n': cfg.num_threads = atoi(optarg);        break;
        case 'd': cfg.duration_sec= atoi(optarg);        break;
        case 'L': cfg.least_mode  = 1;                   break;
        case 'o': cfg.output_csv  = optarg;              break;
        case 'h': usage(argv[0]); exit(0);
        default:  usage(argv[0]); exit(1);
        }
    }

    /* validation */
    if (!cfg.manual && !cfg.trace_path) {
        fprintf(stderr, "ERROR: --trace is required (or use --manual)\n\n");
        usage(argv[0]); exit(1);
    }
    if (cfg.full_trace && cfg.load_buffer >= 0) {
        fprintf(stderr,
            "ERROR: --full-trace and --load-buffer cannot both be set.\n");
        exit(1);
    }
    if (!cfg.manual && cfg.load_mode && !cfg.full_trace && cfg.load_buffer < 0) {
        fprintf(stderr,
            "ERROR: --load requires either --full-trace or --load-buffer N.\n");
        exit(1);
    }
}

/* ── main ────────────────────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    parse_args(argc, argv);
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    /* banner */
    printf("══════════════════════════════════════════════════════════\n");
    printf("LEAST Trace Driver\n");
    if (cfg.manual) {
        printf("Mode     : MANUAL\n");
        printf("Phase    : %s\n", cfg.load_mode ? "LOAD" : "BENCHMARK");
        printf("Keys     : %lu\n", (unsigned long)cfg.num_keys);
        printf("Val size : %d bytes\n", cfg.value_size);
        if (!cfg.load_mode)
            printf("Read%%    : %.0f%% GET / %.0f%% SET\n",
                   cfg.read_ratio*100, (1-cfg.read_ratio)*100);
    } else {
        printf("Trace    : %s\n", cfg.trace_path);
        printf("Phase    : %s\n", cfg.load_mode ? "LOAD (SETs only)" : "BENCHMARK");
        if (cfg.load_mode) {
            if (cfg.full_trace)
                printf("Stop at  : end of file (--full-trace)\n");
            else
                printf("Stop at  : %ds + %ds buffer = %ds\n",
                       cfg.duration_sec, cfg.load_buffer,
                       cfg.duration_sec + cfg.load_buffer);
        } else {
            printf("Duration : %ds (%.1fh)\n",
                   cfg.duration_sec, cfg.duration_sec/3600.0);
            printf("Speed    : %.1fx%s\n", cfg.speed,
                   cfg.speed==0?" (max)":cfg.speed==1?" (real-time)":"");
        }
        printf("TTL      : %s\n", cfg.disable_ttl ? "disabled" : "from trace");
    }
    printf("Host     : %s:%d\n", cfg.hosts, cfg.port);
    printf("Consist  : %s\n", cfg.consistency);
    printf("Threads  : %d\n", cfg.num_threads);
    printf("EC mode  : %s\n", cfg.least_mode
           ? "LEAST (0x00 prefix)" : "Default Cassandra");
    printf("Output   : %s\n", cfg.output_csv);
    printf("══════════════════════════════════════════════════════════\n\n");

    cass_connect();

    /* prescan or manual setup */
    if (cfg.manual) {
        g_max_value_size = cfg.value_size;
        g_total_sets     = cfg.num_keys;
        printf("Manual mode: %lu keys × %d bytes = %.2f GB\n\n",
               (unsigned long)cfg.num_keys, cfg.value_size,
               (double)cfg.num_keys * cfg.value_size / (1024.0*1024.0*1024.0));
    } else {
        prescan_trace();
    }

    /* value buffers */
    g_stats = calloc(cfg.num_threads, sizeof(ThreadStats));
    g_vbufs = malloc(cfg.num_threads * sizeof(char *));
    int bufsz = g_max_value_size + 1;
    for (int i = 0; i < cfg.num_threads; i++) {
        tracker_init(&g_stats[i].get);
        tracker_init(&g_stats[i].set);
        g_vbufs[i] = malloc(bufsz);
        if (!g_vbufs[i]) { perror("malloc vbuf"); exit(1); }
        if (cfg.least_mode) {
            g_vbufs[i][0] = '\x00';
            memset(g_vbufs[i]+1, 'a', bufsz-2);
        } else {
            memset(g_vbufs[i], 'a', bufsz-1);
        }
        g_vbufs[i][bufsz-1] = '\0';
    }
    printf("Value buffer: %d bytes/thread × %d threads = %.1f MB\n\n",
           bufsz, cfg.num_threads,
           (double)bufsz * cfg.num_threads / (1024.0*1024.0));

    if (!cfg.manual)
        queue_init(&g_queue, QUEUE_CAPACITY);

    /* CSV header */
    if (strcmp(cfg.output_csv, "/dev/null") != 0) {
        g_csv = fopen(cfg.output_csv, "w");
        if (!g_csv) perror("fopen output");
        else fprintf(g_csv,
                     "time,elapsed_s,progress_pct,eta,"
                     "get_count,get_mean_us,set_count,set_mean_us,"
                     "errors,ops_per_sec\n");
    }

    g_t_start = now_ns();

    /* start workers */
    Worker *workers = malloc(cfg.num_threads * sizeof(Worker));
    void *(*wfn)(void *) = cfg.manual
        ? (cfg.load_mode ? manual_load_worker : manual_bench_worker)
        : worker_fn;
    for (int i = 0; i < cfg.num_threads; i++) {
        workers[i].id = i;
        pthread_create(&workers[i].tid, NULL, wfn, &workers[i]);
    }

    /* reporter thread */
    pthread_t rep_tid;
    pthread_create(&rep_tid, NULL, reporter_fn, NULL);

    /* duration timer — not needed for manual load (workers self-terminate) */
    pthread_t timer_tid;
    bool timer_started = !(cfg.manual && cfg.load_mode);
    if (timer_started)
        pthread_create(&timer_tid, NULL, timer_fn, NULL);

    if (cfg.manual) {
        for (int i = 0; i < cfg.num_threads; i++)
            pthread_join(workers[i].tid, NULL);
        g_stop = 1;
    } else {
        /* reader runs on main thread */
        run_reader();

        /* reader finished and set g_queue.done = 1.
         * Workers exit naturally when queue is empty AND g_queue.done is set.
         * Just join them — do NOT set g_stop before joining, as that would
         * cause workers to exit before processing all queued records. */
        for (int i = 0; i < cfg.num_threads; i++)
            pthread_join(workers[i].tid, NULL);
        g_stop = 1;   /* now stop reporter */

        /* debug: show per-thread counts and total */
        uint64_t dbg_sets = 0;
        for (int i = 0; i < cfg.num_threads; i++) {
            dbg_sets += g_stats[i].set.count;
            printf("DEBUG thread %2d: sets=%lu\n",
                   i, (unsigned long)g_stats[i].set.count);
        }
        printf("DEBUG total popped from queue: %lu\n",
               (unsigned long)__atomic_load_n(&g_popped, __ATOMIC_RELAXED));
        printf("DEBUG total sets executed: %lu  prescan: %lu  diff: %ld\n",
               (unsigned long)dbg_sets,
               (unsigned long)g_total_sets,
               (long)g_total_sets - (long)dbg_sets);
    }

    pthread_join(rep_tid, NULL);
    if (timer_started) pthread_cancel(timer_tid);

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
