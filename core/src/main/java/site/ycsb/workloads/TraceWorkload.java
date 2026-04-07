/**
 * TraceWorkload.java
 *
 * A YCSB Workload that replays a Twitter-format cache trace file.
 *
 * Trace file format (CSV, no header):
 *   timestamp_sec, key, key_size, value_size, client_id, operation, ttl
 *
 * Supported operations:
 *   get, gets           → READ
 *   set, add, replace   → INSERT
 *   cas                 → UPDATE
 *   append, prepend     → UPDATE
 *   delete              → DELETE
 *   incr, decr          → UPDATE (approximated)
 *
 * Properties:
 *   tracefile                  — path to trace file (required)
 *   trace.valuesize=trace|ycsb — use value_size from trace or fieldlength from
 *                                 properties (default: ycsb)
 *   trace.load.valuesize=trace|ycsb — same but for load phase (default: ycsb)
 *   fieldlength                — used when trace.valuesize=ycsb (default: 100)
 *
 * Usage:
 *   -P workloads/traceworkload -p tracefile=/path/to/trace.csv
 *   -p trace.valuesize=trace -p trace.load.valuesize=ycsb
 *
 * Load phase  (-load): inserts all unique writable keys from the trace as fast
 *                       as possible using the configured thread count.
 * Run  phase  (-t):    replays all trace entries in order, sleeping between
 *                       operations to match original arrival timing.
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class TraceWorkload extends Workload {

  // ── Property keys ──────────────────────────────────────────────────────────
  public static final String TRACE_FILE_PROPERTY        = "tracefile";
  public static final String TRACE_VALUESIZE_PROPERTY   = "trace.valuesize";       // trace | ycsb
  public static final String TRACE_LOAD_VALUESIZE_PROP  = "trace.load.valuesize";  // trace | ycsb
  public static final String FIELD_LENGTH_PROPERTY      = "fieldlength";

  private static final String VALUESIZE_TRACE = "trace";
  private static final String VALUESIZE_YCSB  = "ycsb";
  private static final String FIELD_NAME      = "field0";

  // ── Parsed trace ────────────────────────────────────────────────────────────
  /** One entry per line in the trace file. */
  private static class TraceEntry {
    final double timestampSec;
    final String key;
    final int    valueSize;   // from trace column 3
    final String operation;   // raw string from trace

    TraceEntry(double ts, String key, int valueSize, String op) {
      this.timestampSec = ts;
      this.key          = key;
      this.valueSize    = valueSize;
      this.operation    = op;
    }
  }

  // ── Shared state (set once in init, read-only after that) ──────────────────
  private List<TraceEntry>                 traceEntries;
  /** All write entries from the trace — used for load phase. */
  private List<TraceEntry>                 loadKeys;
  private double                           firstTimestamp;
  private int                              fixedFieldLength;
  private boolean                          runUsesTraceSize;
  private boolean                          loadUsesTraceSize;

  /** Shared atomic index for the run phase across all threads. */
  private static final AtomicInteger       runIndex       = new AtomicInteger(0);
  /** Shared atomic index for the load phase across all threads. */
  private static final AtomicInteger       loadIndex      = new AtomicInteger(0);

  /** Tracks the last progress percentage milestone printed (multiples of PROGRESS_INTERVAL_PCT). */
  private static final AtomicInteger       lastPctPrinted = new AtomicInteger(0);
  /** Print a progress line every N percent. */
  private static final int                 PROGRESS_INTERVAL_PCT = 5;

  /** Wall-clock nanotime when the first doTransaction() call fires. */
  private volatile long                    runStartNs  = 0;
  private volatile long                    loadStartNs = 0;
  private final Object                     startLock   = new Object();
  private volatile boolean                 started     = false;
  private volatile boolean                 loadStarted = false;

  // ── init ────────────────────────────────────────────────────────────────────
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

    System.err.println("[TraceWorkload] Loading trace: " + traceFile);
    System.err.println("[TraceWorkload] Run  value size source : " + runSizeProp);
    System.err.println("[TraceWorkload] Load value size source : " + loadSizeProp);

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
          System.err.println("[TraceWorkload] Skipping malformed line " + lineNum + ": " + line);
          continue;
        }
        try {
          double ts        = Double.parseDouble(parts[0].trim());
          String key       = parts[1].trim();
          int    valueSize = Integer.parseInt(parts[3].trim());
          String op        = parts[5].trim().toLowerCase();

          TraceEntry entry = new TraceEntry(ts, key, valueSize, op);
          traceEntries.add(entry);

          // Collect ALL write entries for load phase (no deduplication)
          if (isWrite(op)) {
            loadKeys.add(entry);
          }
        } catch (NumberFormatException e) {
          System.err.println("[TraceWorkload] Skipping line " + lineNum + " (parse error): " + e.getMessage());
        }
      }
    } catch (IOException e) {
      throw new WorkloadException("Failed to read trace file: " + traceFile, e);
    }

    if (traceEntries.isEmpty()) {
      throw new WorkloadException("Trace file is empty or has no valid entries: " + traceFile);
    }

    firstTimestamp = traceEntries.get(0).timestampSec;

    System.err.println("[TraceWorkload] Loaded " + traceEntries.size()
        + " trace entries, " + loadKeys.size() + " write entries for load phase.");
  }

  // ── Load phase ──────────────────────────────────────────────────────────────
  /**
   * Called repeatedly by worker threads during the load phase.
   * Each call inserts the next write entry from the trace as fast as possible.
   * All write operations are replayed in trace order — no deduplication.
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int idx = loadIndex.getAndIncrement();
    if (idx >= loadKeys.size()) {
      return false; // nothing left to load
    }

    // ── Load phase progress ───────────────────────────────────────────────
    if (!loadStarted) {
      synchronized (startLock) {
        if (!loadStarted) {
          loadStartNs = System.nanoTime();
          loadStarted = true;
          System.err.println("[TraceWorkload] Load phase started. Inserting "
              + loadKeys.size() + " write entries.");
        }
      }
    }
    printProgress("LOAD", idx + 1, loadKeys.size());

    TraceEntry entry = loadKeys.get(idx);
    int valueLen = loadUsesTraceSize ? Math.max(entry.valueSize, 1) : fixedFieldLength;

    HashMap<String, ByteIterator> values = buildValue(valueLen);
    Status status = db.insert("usertable", entry.key, values);
    return status == Status.OK || status == Status.BATCHED_OK;
  }

  // ── Run phase ───────────────────────────────────────────────────────────────
  /**
   * Called repeatedly by worker threads during the run phase.
   * Sleeps until the trace-specified arrival time, then issues the operation.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    int idx = runIndex.getAndIncrement();
    if (idx >= traceEntries.size()) {
      return false; // replay complete
    }

    TraceEntry entry = traceEntries.get(idx);

    // ── Establish wall-clock start on first call ──────────────────────────
    if (!started) {
      synchronized (startLock) {
        if (!started) {
          runStartNs = System.nanoTime();
          started    = true;
          lastPctPrinted.set(0);  // reset so run phase progress starts fresh from 0%
          System.err.println("[TraceWorkload] Run phase started. Replaying "
              + traceEntries.size() + " trace entries.");
        }
      }
    }

    // ── Run phase progress ────────────────────────────────────────────────
    printProgress("RUN", idx + 1, traceEntries.size());

    // ── Sleep until this entry's arrival time ─────────────────────────────
    double offsetSec = entry.timestampSec - firstTimestamp;
    long   targetNs  = runStartNs + (long)(offsetSec * 1_000_000_000L);
    long   nowNs     = System.nanoTime();
    long   sleepNs   = targetNs - nowNs;
    if (sleepNs > 0) {
      LockSupport.parkNanos(sleepNs);
    }

    // ── Issue operation ───────────────────────────────────────────────────
    int valueLen = runUsesTraceSize ? Math.max(entry.valueSize, 1) : fixedFieldLength;
    issueOperation(db, entry, valueLen);

    return true;
  }

  // ── Operation dispatch ───────────────────────────────────────────────────
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
        System.err.println("[TraceWorkload] Unknown operation '" + entry.operation + "' — skipping.");
        break;
    }
  }

  // ── Progress reporting ───────────────────────────────────────────────────
  /**
   * Prints a progress line to stderr every PROGRESS_INTERVAL_PCT percent.
   * Thread-safe via AtomicInteger — only one thread prints each milestone.
   */
  private void printProgress(String phase, int done, int total) {
    int pct = (int)((100L * done) / total);
    int milestone = (pct / PROGRESS_INTERVAL_PCT) * PROGRESS_INTERVAL_PCT;
    if (milestone > 0 && lastPctPrinted.get() < milestone) {
      if (lastPctPrinted.compareAndSet(milestone - PROGRESS_INTERVAL_PCT, milestone)) {
        long startNs   = "RUN".equals(phase) ? runStartNs : loadStartNs;
        long elapsedSec = startNs > 0 ? (System.nanoTime() - startNs) / 1_000_000_000L : 0;
        System.err.printf("[TraceWorkload] [%s] %3d%%  %,d / %,d ops   elapsed: %ds%n",
            phase, milestone, done, total, elapsedSec);
      }
    }
    if (done == total && lastPctPrinted.compareAndSet(
        (100 / PROGRESS_INTERVAL_PCT) * PROGRESS_INTERVAL_PCT - PROGRESS_INTERVAL_PCT, 100)) {
      long startNs    = "RUN".equals(phase) ? runStartNs : loadStartNs;
      long elapsedSec = startNs > 0 ? (System.nanoTime() - startNs) / 1_000_000_000L : 0;
      System.err.printf("[TraceWorkload] [%s] 100%%  %,d / %,d ops  COMPLETE  elapsed: %ds%n",
          phase, done, total, elapsedSec);
    }
  }

  // ── Value construction ───────────────────────────────────────────────────
  private HashMap<String, ByteIterator> buildValue(int valueLen) {
    // Ensure at least 1 byte after prefix
    int totalLen = Math.max(valueLen, 1);
    byte[] data  = new byte[totalLen];
    data[0]      = 0x00;  // LEAST write-path prefix (required by this fork)
    // remaining bytes left as 0x00 — lightweight, no random generation needed
    // If you want random content: ThreadLocalRandom.current().nextBytes(data);

    HashMap<String, ByteIterator> values = new HashMap<>();
    values.put(FIELD_NAME, new ByteArrayByteIterator(data));
    return values;
  }

  // ── Helpers ──────────────────────────────────────────────────────────────
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
