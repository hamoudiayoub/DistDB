package io.distdb.env;

/** The real {@link Clock}, backed by the JVM's monotonic and wall clocks. */
public final class SystemClock implements Clock {

  /** The single instance; the class holds no state. */
  public static final SystemClock INSTANCE = new SystemClock();

  private SystemClock() {}

  @Override
  public long nanoTime() {
    return System.nanoTime();
  }

  @Override
  public long wallTimeMillis() {
    return System.currentTimeMillis();
  }
}
