package site.ycsb;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory pool of cell values loaded from a text file (one value per line,
 * printable ASCII). Used by CoreWorkload (or any other workload) to source
 * values from a pre-prepared dataset instead of generating random bytes.
 *
 * Values are returned in the file's natural line order: the same pool file
 * produces the same value sequence on every run, so experiments are
 * bit-for-bit reproducible.
 *
 * Loaded once per JVM (singleton, identified by file path). The cursor is
 * an AtomicInteger so the hot path is lock-free. When the cursor wraps
 * past the end of the pool, it cycles back to the beginning.
 *
 * Usage from a workload:
 *   ValuePool pool = ValuePool.getInstance(path);
 *   byte[] v = pool.nextValue();
 *   ByteIterator iter = new PoolByteIterator(v);
 */
public final class ValuePool {

  private static volatile ValuePool instance;
  private static final Object INIT_LOCK = new Object();

  private final List<byte[]> values;
  private final AtomicInteger cursor = new AtomicInteger(0);
  private final String sourcePath;

  private ValuePool(String path) throws IOException {
    this.sourcePath = path;
    this.values = loadFile(path);
    if (values.isEmpty()) {
      throw new IOException("Value pool is empty: " + path);
    }
    System.err.printf(
        "[ValuePool] Loaded %,d values (%.2f GB) from %s%n",
        values.size(),
        totalBytes() / 1e9,
        path);
  }

  /**
   * Get the singleton pool, initializing on first call. Subsequent calls
   * return the existing instance regardless of the path argument.
   */
  public static ValuePool getInstance(String path) throws IOException {
    ValuePool local = instance;
    if (local == null) {
      synchronized (INIT_LOCK) {
        local = instance;
        if (local == null) {
          local = new ValuePool(path);
          instance = local;
        }
      }
    }
    return local;
  }

  /**
   * Return the next value, cycling back to the start when the pool is
   * exhausted. The returned byte array is shared -- callers must not
   * modify it. PoolByteIterator does not modify the array.
   */
  public byte[] nextValue() {
    // Mask off the sign bit so we never get a negative index after overflow.
    int idx = (cursor.getAndIncrement() & Integer.MAX_VALUE) % values.size();
    return values.get(idx);
  }

  public int size() {
    return values.size();
  }

  public String getSourcePath() {
    return sourcePath;
  }

  private static List<byte[]> loadFile(String path) throws IOException {
    List<byte[]> result = new ArrayList<>();
    try (BufferedReader reader = Files.newBufferedReader(
        Paths.get(path), StandardCharsets.US_ASCII)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.isEmpty()) {
          result.add(line.getBytes(StandardCharsets.US_ASCII));
        }
      }
    }
    return result;
  }

  private long totalBytes() {
    long total = 0;
    for (byte[] v : values) {
      total += v.length;
    }
    return total;
  }
}
