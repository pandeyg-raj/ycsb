package site.ycsb;

/**
 * ByteIterator backed by a pre-loaded byte array (a single value drawn from
 * a ValuePool). Constructed cheaply -- the byte array is shared with the
 * pool, never copied or modified.
 */
public class PoolByteIterator extends ByteIterator {

  private final byte[] data;
  private int offset = 0;

  public PoolByteIterator(byte[] data) {
    this.data = data;
  }

  @Override
  public boolean hasNext() {
    return offset < data.length;
  }

  @Override
  public byte nextByte() {
    return data[offset++];
  }

  @Override
  public long bytesLeft() {
    return data.length - offset;
  }

  @Override
  public void reset() {
    offset = 0;
  }
}
