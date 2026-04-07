/**
 * TraceWorkload.java
 *
 * Replays a Twitter-format cache trace against Cassandra via YCSB.
 *
 * Trace format (CSV, no header):
 *   timestamp_sec, key, key_size, value_size, client_id, operation, ttl
 *
 * Operation mapping:
 *   get, gets           → READ
 *   set, add, replace   → INSERT
 *   cas, append, prepend, incr, decr → UPDATE
 *   delete              → DELETE
 *
 * Architecture (run phase):
 *   One dispatcher thread reads the trace in order, sleeps until each
 *   entry's wall-clock arrival time, then enqueues it. Multiple worker
 *   threads (YCSB -threads N) pull from the queue and issue immediately.
 *   This correctly handles bursts of 3000+ req/sec without ordering issues.
 *
 * Properties:
 *   tracefile                    path to trace CSV (required)
 *   trace.valuesize=trace|ycsb   run phase value size source (default: ycsb)
 *   trace.load.valuesize=trace|ycsb  load phase value size source (default: ycsb)
 *   fieldlength                  fixed value size when valuesize=ycsb (default: 100)
 *   trace.workers=N              worker thread count hint (informational only; set via -threads)
 */

package site.ycsb.workloads;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;
import site.ycsb.Workload;
import site.ycsb.WorkloadException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class TraceWorkload extends Workload {

  // ── Property keys ────────────────────────────────────────────────────────────
  public static final String TRACE_FILE_PROPERTY       = "tracefile";
  public static final String TRACE_VALUESIZE_PROPERTY  = "trace.valuesize";
  public static final String TRACE_LOAD_VALUESIZE_PROP = "trace.load.valuesize";
  public static final String FIELD_LENGTH_PROPERTY     = "fieldlength";

  private static final String VALUESIZE_TRACE = "trace";
  private static final String VALUESIZE_YCSB  = "ycsb";
  private static final String FIELD_NAME      = "field0";

  /** Sentinel placed in the queue to signal a worker thread to stop. */
  private static final TraceEntry POISON = new TraceEntry(0, "__POISON__", 0, "poison");

  // ── Trace entry ──────────────────────────────────────────────────────────────
  private static class TraceEntry {
    final double timestampSec;
    final String key;
    final int    valueSize;
    final String operation;

    TraceEntry(double ts, String key, int valueSize, String op) {
      this.timestampSec = ts;
      this.key          = key;
      this.valueSize    = valueSize;
      this.operation    = op;
    }
  }

  // ── Shared state ─────────────────────────────────────────────────────────────
  private List<TraceEntry> traceEntries;
  private List<TraceEntry> loadKeys;
  private double           firstTimestamp;
  private double           lastTimestamp;
  private int              fixedFieldLength;
  private boolean          runUsesTraceSize;
  private boolean          loadUsesTraceSize;

  /**
   * Queue between dispatcher and worker threads.
   * Capacity 50000 — large enough to absorb bursts without blocking dispatcher.
   */
  private static final BlockingQueue<TraceEntry> queue = new LinkedBlockingQueue<>(50000);

  /** Number of worker threads — set from YCSB -threads parameter via workaround. */
  private static volatile int workerCount = 1;

  /** Load phase shared index. */
  private static final AtomicInteger loadIndex = new AtomicInteger(0);

  /** Dispatcher thread — started once on first doTransaction() call. */
  private static volatile Thread   dispatcherThread = null;
  private static volatile boolean  dispatcherStarted = false;
  private static final Object      dispatcherLock    = new Object();

  /** Wall-clock nanotime when dispatcher fires the first entry. */
  private static volatile long runStartNs = 0;

  // ── Progress tracking (time-based) ──────────────────────────────────────────
  /** Trace time span in seconds. */
  private double traceDurationSec;
  /** Current trace timestamp being dispatched — updated by dispatcher. */
  private static final AtomicLong currentTraceNs = new AtomicLong(0);
  /** Last progress milestone printed (multiples of PROGRESS_STEP_PCT). */
  private static final AtomicInteger lastPctPrinted = new AtomicInteger(0);
  private static final int PROGRESS_STEP_PCT = 5;

  // ── Load progress ────────────────────────────────────────────────────────────
  private static volatile long loadStartNs  = 0;
  private static volatile boolean loadStarted = false;
  private static final Object loadStartLock = new Object();

  // ── init ─────────────────────────────────────────────────────────────────────
  @Override
  public void init(Properties p) throws WorkloadException {
    String traceFile = p.getProperty(TRACE_FILE_PROPERTY);
    if (traceFile == null || traceFile.isEmpty()) {
      throw new WorkloadException("Property '" + TRACE_FILE_PROPERTY + "' is required.");
    }

    String runSizeProp  = p.getProperty(TRACE_VALUESIZE_PROPERTY,  VALUESIZE_YCSB);
    String loadSizeProp = p.getProperty(TRACE_LOAD_VALUESIZE_PROP, VALUESIZE_YCSB);
    runUsesTraceSize  = VALUESIZE_TRACE.equalsIgnoreCase(runSizeProp);
    loadUsesTraceSize = VALUESIZE_TRACE.equalsIgnoreCase(loadSizeProp);
    fixedFieldLength  = Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, "100"));

    // Grab worker count from properties if specified
    String wc = p.getProperty("threadcount", p.getProperty("trace.workers", "1"));
    try { workerCount = Integer.parseInt(wc); } catch (NumberFormatException e) { workerCount = 1; }

    System.err.println("[TraceWorkload] Loading trace: " + traceFile);
    System.err.println("[TraceWorkload] Run value size : " + runSizeProp
        + "  Load value size: " + loadSizeProp);

    traceEntries = new ArrayList<>();
    loadKeys     = new ArrayList<>();

    try (BufferedReader br = new BufferedReader(new FileReader(traceFile))) {
      String line;
      int lineNum = 0;
      while ((line = br.readLine()) != null) {
        lineNum++;
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        String[] parts = line.split(",", -1);
        if (parts.length < 6) {
          System.err.println("[TraceWorkload] Skipping malformed line " + lineNum);
          continue;
        }
        try {
          double ts        = Double.parseDouble(parts[0].trim());
          String key       = parts[1].trim();
          int    valueSize = Integer.parseInt(parts[3].trim());
          String op        = parts[5].trim().toLowerCase();

          TraceEntry entry = new TraceEntry(ts, key, valueSize, op);
          traceEntries.add(entry);
          if (isWrite(op)) {
            loadKeys.add(entry);
          }
        } catch (NumberFormatException e) {
          System.err.println("[TraceWorkload] Parse error at line " + lineNum + ": " + e.getMessage());
        }
      }
    } catch (IOException e) {
      throw new WorkloadException("Cannot read trace file: " + traceFile, e);
    }

    if (traceEntries.isEmpty()) {
      throw new WorkloadException("Trace file has no valid entries: " + traceFile);
    }

    firstTimestamp   = traceEntries.get(0).timestampSec;
    lastTimestamp    = traceEntries.get(traceEntries.size() - 1).timestampSec;
    traceDurationSec = lastTimestamp - firstTimestamp;

    System.err.println("[TraceWorkload] Trace entries: " + traceEntries.size()
        + "  Write entries for load: " + loadKeys.size());
    System.err.printf("[TraceWorkload] Trace duration: %.0f seconds (%.1f hours)%n",
        traceDurationSec, traceDurationSec / 3600.0);
  }

  // ── Load phase ───────────────────────────────────────────────────────────────
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int idx = loadIndex.getAndIncrement();
    if (idx >= loadKeys.size()) {
      return false;
    }
    if (!loadStarted) {
      synchronized (loadStartLock) {
        if (!loadStarted) {
          loadStartNs = System.nanoTime();
          loadStarted = true;
          System.err.println("[TraceWorkload] Load phase started. Inserting "
              + loadKeys.size() + " write entries.");
        }
      }
    }

    // Progress based on op count (load is as-fast-as-possible so time doesn't help)
    printLoadProgress(idx + 1, loadKeys.size());

    TraceEntry entry = loadKeys.get(idx);
    int valueLen = loadUsesTraceSize ? Math.max(entry.valueSize, 1) : fixedFieldLength;
    Status status = db.insert("usertable", entry.key, buildValue(valueLen));
    return status == Status.OK || status == Status.BATCHED_OK;
  }

  // ── Run phase: worker ────────────────────────────────────────────────────────
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    // Start dispatcher once
    if (!dispatcherStarted) {
      synchronized (dispatcherLock) {
        if (!dispatcherStarted) {
          dispatcherStarted = true;
          lastPctPrinted.set(0);
          startDispatcher();
        }
      }
    }

    // Pull next entry from queue — block up to 5s waiting for dispatcher
    TraceEntry entry;
    try {
      entry = queue.poll(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }

    if (entry == null || entry == POISON) {
      return false; // trace exhausted
    }

    int valueLen = runUsesTraceSize ? Math.max(entry.valueSize, 1) : fixedFieldLength;
    issueOperation(db, entry, valueLen);
    return true;
  }

  // ── Dispatcher thread ────────────────────────────────────────────────────────
  private void startDispatcher() {
    dispatcherThread = new Thread(() -> {
      runStartNs = System.nanoTime();
      System.err.println("[TraceWorkload] Run phase started. Replaying "
          + traceEntries.size() + " entries over "
          + String.format("%.0f", traceDurationSec) + "s of trace time."
          + " Worker threads: " + workerCount);

      for (TraceEntry entry : traceEntries) {
        // ── Timing: sleep until this entry's trace time ────────────────────
        double offsetSec = entry.timestampSec - firstTimestamp;
        long   targetNs  = runStartNs + (long)(offsetSec * 1_000_000_000L);
        long   sleepNs   = targetNs - System.nanoTime();
        if (sleepNs > 0) {
          LockSupport.parkNanos(sleepNs);
        }

        // ── Update current trace time for progress reporting ───────────────
        currentTraceNs.set((long)((entry.timestampSec - firstTimestamp) * 1_000_000_000L));
        printRunProgress(entry.timestampSec);

        // ── Enqueue — block if workers are falling behind ──────────────────
        try {
          queue.put(entry);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }

      // Send one POISON pill per worker thread so all workers stop cleanly
      System.err.println("[TraceWorkload] Dispatcher done. Sending stop signals to "
          + workerCount + " workers.");
      for (int i = 0; i < workerCount; i++) {
        try {
          queue.put(POISON);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }, "TraceDispatcher");

    dispatcherThread.setDaemon(true);
    dispatcherThread.start();
  }

  // ── Operation dispatch ───────────────────────────────────────────────────────
  private void issueOperation(DB db, TraceEntry entry, int valueLen) {
    switch (entry.operation) {
      case "get":
      case "gets":
        db.read("usertable", entry.key, null, new HashMap<>());
        break;
      case "set":
      case "add":
      case "replace":
        db.insert("usertable", entry.key, buildValue(valueLen));
        break;
      case "cas":
      case "append":
      case "prepend":
      case "incr":
      case "decr":
        db.update("usertable", entry.key, buildValue(valueLen));
        break;
      case "delete":
        db.delete("usertable", entry.key);
        break;
      default:
        System.err.println("[TraceWorkload] Unknown op: " + entry.operation);
    }
  }

  // ── Progress: time-based for run phase ──────────────────────────────────────
  private void printRunProgress(double currentTraceSec) {
    if (traceDurationSec <= 0) {
      return;
    }
    int pct = (int)(100.0 * (currentTraceSec - firstTimestamp) / traceDurationSec);
    int milestone = (pct / PROGRESS_STEP_PCT) * PROGRESS_STEP_PCT;
    if (milestone > 0 && lastPctPrinted.get() < milestone) {
      if (lastPctPrinted.compareAndSet(milestone - PROGRESS_STEP_PCT, milestone)) {
        long elapsedSec = (System.nanoTime() - runStartNs) / 1_000_000_000L;
        int  queueDepth = queue.size();
        System.err.printf(
            "[TraceWorkload] [RUN] %3d%%  trace_time=%.0fs / %.0fs"
            + "  wall_elapsed=%ds  queue_depth=%d%n",
            milestone,
            currentTraceSec - firstTimestamp,
            traceDurationSec,
            elapsedSec,
            queueDepth);
      }
    }
    // 100% complete
    if (currentTraceSec >= lastTimestamp
        && lastPctPrinted.compareAndSet(100 - PROGRESS_STEP_PCT, 100)) {
      long elapsedSec = (System.nanoTime() - runStartNs) / 1_000_000_000L;
      System.err.printf(
          "[TraceWorkload] [RUN] 100%%  COMPLETE  wall_elapsed=%ds%n", elapsedSec);
    }
  }

  // ── Progress: op-count-based for load phase ──────────────────────────────────
  private void printLoadProgress(int done, int total) {
    int pct      = (int)(100L * done / total);
    int milestone = (pct / PROGRESS_STEP_PCT) * PROGRESS_STEP_PCT;
    if (milestone > 0 && lastPctPrinted.get() < milestone) {
      if (lastPctPrinted.compareAndSet(milestone - PROGRESS_STEP_PCT, milestone)) {
        long elapsedSec = loadStartNs > 0 ? (System.nanoTime() - loadStartNs) / 1_000_000_000L : 0;
        System.err.printf("[TraceWorkload] [LOAD] %3d%%  %,d / %,d ops  elapsed=%ds%n",
            milestone, done, total, elapsedSec);
      }
    }
    if (done == total) {
      long elapsedSec = loadStartNs > 0 ? (System.nanoTime() - loadStartNs) / 1_000_000_000L : 0;
      System.err.printf("[TraceWorkload] [LOAD] 100%%  %,d ops  COMPLETE  elapsed=%ds%n",
          total, elapsedSec);
    }
  }

  // ── Value construction ───────────────────────────────────────────────────────
  private HashMap<String, ByteIterator> buildValue(int valueLen) {
    int totalLen = Math.max(valueLen, 1);
    byte[] data  = new byte[totalLen];
    data[0]      = 0x00; // required prefix for LEAST write path
    HashMap<String, ByteIterator> values = new HashMap<>();
    values.put(FIELD_NAME, new ByteArrayByteIterator(data));
    return values;
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────
  private static boolean isWrite(String op) {
    switch (op) {
      case "set": case "add": case "replace":
      case "cas": case "append": case "prepend":
        return true;
      default:
        return false;
    }
  }
}
