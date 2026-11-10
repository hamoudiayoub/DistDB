package io.distdb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An ordered list of puts and deletes applied atomically by {@link KvEngine#write}. If the same key
 * appears more than once, the last operation wins. Keys and values are copied when added. Not
 * thread-safe.
 */
public final class WriteBatch {

  private final List<Op> ops = new ArrayList<>();
  private long approximateBytes;

  /** Sets {@code key} to {@code value}. */
  public WriteBatch put(byte[] key, byte[] value) {
    ops.add(new Op(false, key.clone(), value.clone()));
    approximateBytes += key.length + value.length + 16L;
    return this;
  }

  /** Removes {@code key} if present. */
  public WriteBatch delete(byte[] key) {
    ops.add(new Op(true, key.clone(), null));
    approximateBytes += key.length + 16L;
    return this;
  }

  /** The number of operations. */
  public int count() {
    return ops.size();
  }

  public boolean isEmpty() {
    return ops.isEmpty();
  }

  /** Roughly how many bytes the batch occupies. */
  public long approximateBytes() {
    return approximateBytes;
  }

  /** Visits the operations in order. */
  public void forEach(Visitor visitor) {
    Objects.requireNonNull(visitor);
    for (Op op : ops) {
      if (op.delete) {
        visitor.delete(op.key);
      } else {
        visitor.put(op.key, op.value);
      }
    }
  }

  /** Receives a batch's operations. The arrays must not be modified. */
  public interface Visitor {
    void put(byte[] key, byte[] value);

    void delete(byte[] key);
  }

  private record Op(boolean delete, byte[] key, byte[] value) {}
}
