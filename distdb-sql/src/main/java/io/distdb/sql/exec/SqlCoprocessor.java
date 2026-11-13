package io.distdb.sql.exec;

import io.distdb.rpc.Coprocessor;

/**
 * The SQL layer's coprocessor: what a range runs for a pushed-down {@link Fragment}. A server plugs
 * it into every store ({@code Store.coprocessor}), so a range evaluates filters and partial
 * aggregates with the same code the gateway uses.
 */
public final class SqlCoprocessor implements Coprocessor {

  @Override
  public Run start(byte[] program) {
    Fragment f;
    try {
      f = Fragment.decode(program);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
    Fragment.Run run = new Fragment.Run(f);
    return new Run() {
      @Override
      public boolean accept(byte[] key, byte[] value) {
        return run.accept(key, value);
      }

      @Override
      public byte[] finish() {
        return run.finish();
      }

      @Override
      public boolean satisfied() {
        return run.satisfied();
      }
    };
  }
}
