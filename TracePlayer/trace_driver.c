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
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <getopt.h>

#include <cassandra.h>

/* ── Constants ──────────────────────────────────────────────────────────────── */

#define MAX_KEY_LEN     256
#define MAX_VALUE_SIZE  (512 * 1024)   /* 512 KB cap — well above any trace value */
#define QUEUE_CAPACITY  8192           /* must be power of 2 */
#define REPORT_INTERVAL 10             /* seconds between progress prints */
#define MAX_SAMPLES     2000000        /* per-thread latency samples */

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

typedef struct {
    const char *trace_path;
    const char *hosts;          /* comma-separated */
    int         port;
    const char *keyspace;
    const char *table;
    int         num_threads;
    int         duration_sec;
    double      speed;          /* replay speed vs real-time; 0 = max rate */
    const char *output_csv;
    int         least_mode;     /* 1 = prepend 0x00 to field0 (LEAST EC trigger) */
    int         disable_ttl;    /* 1 = write all data with no TTL (grows WSS to disk) */
} Config;

static Config cfg = {
    .trace_path  = NULL,
    .hosts       = "127.0.0.1",
    .port        = 9042,
    .keyspace    = "ycsb",
    .table       = "usertable",
    .num_threads = 32,
    .duration_sec= 300,
    .speed       = 1.0,
    .output_csv  = "latency.csv",
    .least_mode  = 0,
    .disable_ttl = 0,
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

/* ── Cassandra setup ─────────────────────────────────────────────────────────── */

static void cass_die(const char *ctx, CassFuture *f) {
    const char *msg; size_t len;
    cass_future_error_message(f, &msg, &len);
    fprintf(stderr, "ERROR [%s]: %.*s\n", ctx, (int)len, msg);
    cass_future_free(f);
    exit(1);
}

static void cass_connect(void) {
    g_cluster = cass_cluster_new();
    g_session = cass_session_new();

    cass_cluster_set_contact_points(g_cluster, cfg.hosts);
    cass_cluster_set_port(g_cluster, cfg.port);
    cass_cluster_set_num_threads_io(g_cluster, 4);
    cass_cluster_set_queue_size_io(g_cluster, 65536);
    cass_cluster_set_consistency(g_cluster, CASS_CONSISTENCY_LOCAL_QUORUM);

    CassFuture *f = cass_session_connect_keyspace(g_session, g_cluster,
                                                   cfg.keyspace);
    cass_future_wait(f);
    if (cass_future_error_code(f) != CASS_OK)
        cass_die("connect", f);
    cass_future_free(f);

    printf("Connected to %s:%d  keyspace=%s\n",
           cfg.hosts, cfg.port, cfg.keyspace);

    /* Ensure the table exists with exact YCSB schema */
    {
        char q[1024];
        snprintf(q, sizeof(q),
            "CREATE TABLE IF NOT EXISTS %s.%s ("
            "  y_id   varchar,"
            "  field0 varchar,"
            "  PRIMARY KEY (y_id)"
            ") WITH caching     = {'keys':'NONE','rows_per_partition':'none'}"
            "  AND  compression = {'enabled':false}"
            "  AND  read_repair = 'NONE'",
            cfg.keyspace, cfg.table);
        f = cass_session_execute(g_session, cass_statement_new(q, 0));
        cass_future_wait(f);
        /* Non-fatal if table already exists */
        cass_future_free(f);
        printf("Table %s.%s ready\n", cfg.keyspace, cfg.table);
    }

    /* Prepare SELECT */
    {
        char q[256];
        snprintf(q, sizeof(q),
                 "SELECT field0 FROM %s WHERE y_id = ?", cfg.table);
        f = cass_session_prepare(g_session, q);
        cass_future_wait(f);
        if (cass_future_error_code(f) != CASS_OK) cass_die("prepare SELECT", f);
        g_select = cass_future_get_prepared(f);
        cass_future_free(f);
    }

    /* Prepare INSERT (no TTL — data is permanent with --disable-ttl) */
    {
        char q[256];
        snprintf(q, sizeof(q),
                 "INSERT INTO %s (y_id, field0) VALUES (?, ?)", cfg.table);
        f = cass_session_prepare(g_session, q);
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
            if (vsz <= 0)             vsz = 1;
            if (vsz > MAX_VALUE_SIZE) vsz = MAX_VALUE_SIZE;

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

static void run_reader(void) {
    FILE *fp = fopen(cfg.trace_path, "r");
    if (!fp) { perror("fopen trace"); exit(1); }

    char     line[1024];
    uint64_t first_trace_ts = 0;
    uint64_t start_wall     = now_ns();
    bool     first          = true;
    uint64_t submitted = 0, skipped = 0;

    while (!g_stop && fgets(line, sizeof(line), fp)) {
        /* honour duration limit */
        if (now_ns() - start_wall > (uint64_t)cfg.duration_sec * 1000000000ULL)
            break;

        char tmp[1024];
        strncpy(tmp, line, sizeof(tmp) - 1);
        TraceRecord rec;
        if (parse_line(tmp, &rec) < 0 || rec.op == OP_SKIP) {
            skipped++;
            continue;
        }

        /*
         * Pacing: when --speed > 0, sleep so wall-clock elapsed time
         * matches (trace elapsed time / speed).
         *
         * speed=1.0  → real-time replay
         * speed=2.0  → 2× faster than original
         * speed=0    → no pacing, saturate as fast as possible (load phase)
         */
        if (cfg.speed > 0.0) {
            if (first) { first_trace_ts = rec.timestamp; first = false; }
            uint64_t trace_ns = (uint64_t)(
                (double)(rec.timestamp - first_trace_ts) / cfg.speed * 1e9);
            uint64_t wall_ns  = now_ns() - start_wall;
            if (trace_ns > wall_ns) sleep_ns(trace_ns - wall_ns);
        }

        /* spin if queue is full */
        while (!g_stop && !queue_push(&g_queue, &rec))
            sleep_ns(10000);  /* 10 µs */

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
    double ops_s   = (gc + sc) / elapsed;
    double gmean   = gc ? gs / gc : 0.0;
    double smean   = sc ? ss / sc : 0.0;

    char tbuf[16];
    time_t now = time(NULL);
    strftime(tbuf, sizeof(tbuf), "%H:%M:%S", localtime(&now));

    printf("[%s] t=%.0fs | GET n=%lu mean=%.0fµs | "
           "SET n=%lu mean=%.0fµs | err=%lu | %.0f ops/s\n",
           tbuf, elapsed,
           (unsigned long)gc, gmean,
           (unsigned long)sc, smean,
           (unsigned long)(ge + se), ops_s);
    fflush(stdout);

    if (g_csv) {
        fprintf(g_csv, "%s,%.1f,%lu,%.1f,%lu,%.1f,%lu,%.0f\n",
                tbuf, elapsed,
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
    printf("  --host     IP     contact point(s), comma-separated (default: 127.0.0.1)\n");
    printf("  --port     N      CQL port (default: 9042)\n");
    printf("  --keyspace NAME   keyspace (default: ycsb)\n");
    printf("  --table    NAME   table (default: usertable)\n\n");
    printf("Workload:\n");
    printf("  --threads  N      concurrent workers (default: 32)\n");
    printf("  --duration N      seconds to run (default: 300)\n");
    printf("  --speed    F      replay speed vs real-time (default: 1.0)\n");
    printf("                    0 = ignore timestamps, run at max rate (load phase)\n\n");
    printf("LEAST / data size:\n");
    printf("  --least           prepend 0x00 to field0 (LEAST EC trigger)\n");
    printf("  --disable-ttl     ignore trace TTLs, write data permanently\n");
    printf("                    use this to push working set past RAM limit\n\n");
    printf("Output:\n");
    printf("  --output   PATH   latency timeseries CSV (default: latency.csv)\n\n");
    printf("Examples:\n");
    printf("  # Load phase (no timing, max speed)\n");
    printf("  %s --trace cluster46.0 --host 10.10.1.1 \\\n", prog);
    printf("       --least --disable-ttl --speed 0 --output /dev/null\n\n");
    printf("  # Benchmark phase\n");
    printf("  %s --trace cluster46.0 --host 10.10.1.1 \\\n", prog);
    printf("       --least --disable-ttl --speed 1.0 \\\n");
    printf("       --duration 300 --output least_c46.csv\n");
}

static void parse_args(int argc, char **argv) {
    static struct option opts[] = {
        {"trace",       required_argument, 0, 't'},
        {"host",        required_argument, 0, 'H'},
        {"port",        required_argument, 0, 'p'},
        {"keyspace",    required_argument, 0, 'k'},
        {"table",       required_argument, 0, 'T'},
        {"threads",     required_argument, 0, 'n'},
        {"duration",    required_argument, 0, 'd'},
        {"speed",       required_argument, 0, 's'},
        {"least",       no_argument,       0, 'L'},
        {"disable-ttl", no_argument,       0, 'X'},
        {"output",      required_argument, 0, 'o'},
        {"help",        no_argument,       0, 'h'},
        {0,0,0,0}
    };
    int c, idx;
    while ((c = getopt_long(argc, argv, "t:H:p:k:T:n:d:s:LXo:h",
                            opts, &idx)) != -1) {
        switch (c) {
        case 't': cfg.trace_path  = optarg;       break;
        case 'H': cfg.hosts       = optarg;       break;
        case 'p': cfg.port        = atoi(optarg); break;
        case 'k': cfg.keyspace    = optarg;       break;
        case 'T': cfg.table       = optarg;       break;
        case 'n': cfg.num_threads = atoi(optarg); break;
        case 'd': cfg.duration_sec= atoi(optarg); break;
        case 's': cfg.speed       = atof(optarg); break;
        case 'L': cfg.least_mode  = 1;            break;
        case 'X': cfg.disable_ttl = 1;            break;
        case 'o': cfg.output_csv  = optarg;       break;
        case 'h': usage(argv[0]); exit(0);
        default:  usage(argv[0]); exit(1);
        }
    }
    if (!cfg.trace_path) {
        fprintf(stderr, "ERROR: --trace is required\n\n");
        usage(argv[0]); exit(1);
    }
}

/* ── main ────────────────────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    parse_args(argc, argv);
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    printf("══════════════════════════════════════════════════════════\n");
    printf("LEAST Trace Driver\n");
    printf("Trace    : %s\n", cfg.trace_path);
    printf("Host     : %s:%d\n", cfg.hosts, cfg.port);
    printf("Keyspace : %s.%s\n", cfg.keyspace, cfg.table);
    printf("Threads  : %d\n", cfg.num_threads);
    printf("Duration : %ds\n", cfg.duration_sec);
    printf("Speed    : %.1fx%s\n", cfg.speed, cfg.speed==0?" (max rate)":"");
    printf("Mode     : %s\n", cfg.least_mode
           ? "LEAST  (0x00 prefix on field0)"
           : "Default Cassandra (no prefix)");
    printf("TTL      : %s\n", cfg.disable_ttl
           ? "disabled  (data permanent, working set grows to disk)"
           : "from trace");
    printf("Output   : %s\n", cfg.output_csv);
    printf("══════════════════════════════════════════════════════════\n\n");

    cass_connect();

    /* Allocate per-thread resources */
    g_stats = calloc(cfg.num_threads, sizeof(ThreadStats));
    g_vbufs = malloc(cfg.num_threads * sizeof(char *));
    for (int i = 0; i < cfg.num_threads; i++) {
        tracker_init(&g_stats[i].get);
        tracker_init(&g_stats[i].set);

        g_vbufs[i] = malloc(MAX_VALUE_SIZE + 1);
        if (!g_vbufs[i]) { perror("malloc vbuf"); exit(1); }

        if (cfg.least_mode) {
            /*
             * LEAST EC trigger: 0x00 at byte 0, rest is 'a'.
             * Total stored = value_size bytes (trace value_size unchanged).
             *
             *   vbuf[0]        = 0x00  ← LEAST reads this to decide EC
             *   vbuf[1..N-1]   = 'a'
             */
            g_vbufs[i][0] = '\x00';
            memset(g_vbufs[i] + 1, 'a', MAX_VALUE_SIZE - 1);
        } else {
            /* Plain Cassandra: all 'a' */
            memset(g_vbufs[i], 'a', MAX_VALUE_SIZE);
        }
        g_vbufs[i][MAX_VALUE_SIZE] = '\0';
    }

    queue_init(&g_queue, QUEUE_CAPACITY);

    /* CSV header */
    if (cfg.output_csv && strcmp(cfg.output_csv, "/dev/null") != 0) {
        g_csv = fopen(cfg.output_csv, "w");
        if (!g_csv) perror("fopen output");
        else fprintf(g_csv,
                     "time,elapsed_s,get_count,get_mean_us,"
                     "set_count,set_mean_us,errors,ops_per_sec\n");
    }

    g_t_start = now_ns();

    /* Start workers */
    Worker *workers = malloc(cfg.num_threads * sizeof(Worker));
    for (int i = 0; i < cfg.num_threads; i++) {
        workers[i].id = i;
        pthread_create(&workers[i].tid, NULL, worker_fn, &workers[i]);
    }

    /* Start reporter and duration timer */
    pthread_t rep_tid, timer_tid;
    pthread_create(&rep_tid,   NULL, reporter_fn, NULL);
    pthread_create(&timer_tid, NULL, timer_fn,    NULL);

    /* Run trace reader on main thread */
    run_reader();

    /* Wait for queue to drain */
    while (!g_stop) {
        if (__atomic_load_n(&g_queue.head, __ATOMIC_RELAXED) ==
            __atomic_load_n(&g_queue.tail, __ATOMIC_RELAXED)) break;
        sleep_ns(1000000);  /* 1 ms */
    }
    g_stop = 1;

    for (int i = 0; i < cfg.num_threads; i++)
        pthread_join(workers[i].tid, NULL);
    pthread_join(rep_tid, NULL);
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
