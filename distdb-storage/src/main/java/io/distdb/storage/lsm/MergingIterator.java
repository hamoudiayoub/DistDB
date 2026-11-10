package io.distdb.storage.lsm;

import java.util.List;
import java.util.PriorityQueue;

/**
 * Merges several sorted {@link InternalIterator}s into one sorted stream with a min-heap. Internal
 * keys are unique, so there are no duplicates to resolve; ties (which would indicate a bug) are
 * broken by child position so the order stays deterministic.
 */
final class MergingIterator implements InternalIterator {

  private final List<InternalIterator> children;
  private final PriorityQueue<Integer> heap;
  private int current = -1;

  MergingIterator(List<InternalIterator> children) {
    this.children = List.copyOf(children);
    this.heap =
        new PriorityQueue<>(
            Math.max(1, children.size()),
            (a, b) -> {
              int c =
                  InternalKey.COMPARATOR.compare(
                      this.children.get(a).key(), this.children.get(b).key());
              return c != 0 ? c : Integer.compare(a, b);
            });
  }

  @Override
  public boolean valid() {
    return current >= 0;
  }

  @Override
  public void seekToFirst() {
    for (InternalIterator c : children) {
      c.seekToFirst();
    }
    rebuild();
  }

  @Override
  public void seek(byte[] target) {
    for (InternalIterator c : children) {
      c.seek(target);
    }
    rebuild();
  }

  @Override
  public void next() {
    InternalIterator c = children.get(current);
    c.next();
    if (c.valid()) {
      heap.add(current);
    }
    Integer top = heap.poll();
    current = top == null ? -1 : top;
  }

  @Override
  public byte[] key() {
    return children.get(current).key();
  }

  @Override
  public byte[] value() {
    return children.get(current).value();
  }

  @Override
  public void close() {
    for (InternalIterator c : children) {
      c.close();
    }
  }

  private void rebuild() {
    heap.clear();
    for (int i = 0; i < children.size(); i++) {
      if (children.get(i).valid()) {
        heap.add(i);
      }
    }
    Integer top = heap.poll();
    current = top == null ? -1 : top;
  }
}
