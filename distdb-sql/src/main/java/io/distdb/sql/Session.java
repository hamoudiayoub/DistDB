package io.distdb.sql;

import io.distdb.sql.catalog.Catalog;
import io.distdb.sql.exec.ExecContext;
import io.distdb.sql.exec.Operators;
import io.distdb.sql.exec.Writes;
import io.distdb.sql.kv.KvTxn;
import io.distdb.sql.parse.Ast;
import io.distdb.sql.parse.Parser;
import io.distdb.sql.plan.Binder;
import io.distdb.sql.plan.Bound;
import io.distdb.sql.plan.Plan;
import io.distdb.sql.plan.PlanPrinter;
import io.distdb.sql.plan.Planner;
import io.distdb.sql.types.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * One client's session: statements in order, and the transaction they run in.
 *
 * <ul>
 *   <li><b>Autocommit:</b> outside {@code BEGIN}, each statement is a transaction of its own. When
 *       it fails in a way a retry may fix (SQLSTATE {@code 40001}: a conflict, an abort, a fence),
 *       it runs again from scratch, up to ten times; nothing was returned yet, so this is safe.
 *   <li><b>Explicit:</b> after {@code BEGIN}, statements share one snapshot and see their own
 *       writes. After an error the transaction is aborted: statements fail with {@code 25P02} until
 *       {@code ROLLBACK} (or {@code COMMIT}, which then rolls back), as in PostgreSQL.
 * </ul>
 *
 * Not thread-safe: one connection, one session.
 */
public final class Session {

  /** Where a session stands, as pgwire's ReadyForQuery reports it. */
  public enum TxnState {
    /** No transaction: 'I'. */
    IDLE,
    /** In a transaction block: 'T'. */
    OPEN,
    /** In a failed transaction block: 'E'. */
    FAILED
  }

  private static final int AUTOCOMMIT_ATTEMPTS = 10;

  private static final Map<String, String> DEFAULTS =
      Map.ofEntries(
          Map.entry("server_version", "16.0"),
          Map.entry("server_version_num", "160000"),
          Map.entry("server_encoding", "UTF8"),
          Map.entry("client_encoding", "UTF8"),
          Map.entry("datestyle", "ISO, MDY"),
          Map.entry("timezone", "UTC"),
          Map.entry("integer_datetimes", "on"),
          Map.entry("standard_conforming_strings", "on"),
          Map.entry("transaction_isolation", "repeatable read"),
          Map.entry("default_transaction_isolation", "repeatable read"),
          Map.entry("transaction_read_only", "off"),
          Map.entry("search_path", "public"),
          Map.entry("application_name", ""),
          Map.entry("extra_float_digits", "1"),
          Map.entry("max_identifier_length", "63"),
          Map.entry("is_superuser", "on"),
          Map.entry("intervalstyle", "postgres"),
          Map.entry("distdb_pushdown", "on"));

  private final Engine engine;
  private final Map<String, String> vars = new HashMap<>();
  private KvTxn txn;
  private TxnState state = TxnState.IDLE;

  Session(Engine engine) {
    this.engine = engine;
  }

  public TxnState state() {
    return state;
  }

  /** A setting's value, as {@code SHOW} would give it; null if unknown. */
  public String setting(String name) {
    String key = name.toLowerCase(Locale.ROOT);
    return vars.getOrDefault(key, DEFAULTS.get(key));
  }

  /** The settings a server reports when a client connects. */
  public static Map<String, String> startupSettings() {
    Map<String, String> out = new java.util.LinkedHashMap<>();
    for (String k :
        List.of(
            "server_version",
            "server_encoding",
            "client_encoding",
            "DateStyle",
            "TimeZone",
            "integer_datetimes",
            "standard_conforming_strings",
            "is_superuser",
            "IntervalStyle",
            "application_name")) {
      out.put(k, DEFAULTS.get(k.toLowerCase(Locale.ROOT)));
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Simple query

  /**
   * Runs every statement of {@code sql} in order, giving each result to {@code sink}; stops at the
   * first error, which it throws.
   */
  public void execute(String sql, Consumer<Result> sink) {
    List<Ast.Statement> statements;
    try {
      statements = Parser.parse(sql);
    } catch (SqlException e) {
      fail();
      throw e;
    }
    for (Ast.Statement s : statements) {
      sink.accept(run(s, null, new Object[0]));
    }
  }

  /** Runs every statement of {@code sql}; returns their results. */
  public List<Result> execute(String sql) {
    List<Result> out = new ArrayList<>();
    execute(sql, out::add);
    return out;
  }

  /** Runs one statement and returns its result. */
  public Result executeOne(String sql) {
    List<Result> all = execute(sql);
    if (all.size() != 1) {
      throw new IllegalArgumentException("expected one statement: " + sql);
    }
    return all.get(0);
  }

  // -----------------------------------------------------------------------------------------------
  // Extended query

  /**
   * Parses and binds one statement with parameters {@code $1, $2, ...}; {@code paramHints} are the
   * types a client declared (UNKNOWN: infer from the statement).
   */
  public Prepared prepare(String sql, List<Type> paramHints) {
    List<Ast.Statement> statements = Parser.parse(sql);
    if (statements.size() > 1) {
      throw new SqlException(
          SqlState.SYNTAX_ERROR, "cannot insert multiple commands into a prepared statement");
    }
    Prepared p = new Prepared(sql, statements.isEmpty() ? null : statements.get(0), paramHints);
    if (p.statement == null) {
      p.paramTypes = List.of();
      return p;
    }
    describe(p);
    return p;
  }

  /** Runs a prepared statement with {@code params} (values of its parameter types). */
  public Result execute(Prepared p, List<Object> params) {
    if (p.statement == null) {
      return Result.command("");
    }
    return run(p.statement, p, params.toArray());
  }

  private void describe(Prepared p) {
    switch (p.statement) {
      case Ast.QueryStatement q -> bindFor(p);
      case Ast.Insert i -> bindFor(p);
      case Ast.Update u -> bindFor(p);
      case Ast.Delete d -> bindFor(p);
      case Ast.ShowVar s -> {
        p.paramTypes = List.of();
        p.columns = List.of(new Result.Column(s.name(), Type.TEXT));
      }
      case Ast.Explain e -> {
        p.paramTypes = List.of();
        p.columns = List.of(new Result.Column("QUERY PLAN", Type.TEXT));
      }
      default -> {
        p.paramTypes = List.of();
        p.columns = null;
      }
    }
  }

  /** Binds a prepared DML statement or query against the catalog, to learn its types. */
  private void bindFor(Prepared p) {
    Function<KvTxn, Void> work =
        t -> {
          Catalog.Snapshot snapshot = engine.catalog().load(t);
          Binder binder = new Binder(snapshot, p.hints);
          p.bound = binder.bind(p.statement);
          p.catalogVersion = snapshot.version();
          p.paramTypes = binder.paramTypes();
          p.columns = columnsOf(p.bound);
          return null;
        };
    if (state == TxnState.OPEN) {
      work.apply(txn);
    } else {
      KvTxn t = engine.store().begin();
      try {
        work.apply(t);
      } finally {
        rollbackQuietly(t);
      }
    }
  }

  private static List<Result.Column> columnsOf(Bound b) {
    List<Plan.Col> cols =
        switch (b) {
          case Bound.Query q -> q.plan().columns();
          case Bound.Insert i -> i.returning().present() ? i.returning().columns() : null;
          case Bound.Update u -> u.returning().present() ? u.returning().columns() : null;
          case Bound.Delete d -> d.returning().present() ? d.returning().columns() : null;
        };
    if (cols == null) {
      return null;
    }
    List<Result.Column> out = new ArrayList<>();
    for (Plan.Col c : cols) {
      out.add(new Result.Column(c.name(), c.type() == Type.UNKNOWN ? Type.TEXT : c.type()));
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Statements

  private Result run(Ast.Statement s, Prepared p, Object[] params) {
    if (state == TxnState.FAILED && !(s instanceof Ast.Rollback) && !(s instanceof Ast.Commit)) {
      throw new SqlException(
          SqlState.IN_FAILED_SQL_TRANSACTION,
          "current transaction is aborted, commands ignored until end of transaction block");
    }
    try {
      return switch (s) {
        case Ast.Begin b -> begin();
        case Ast.Commit c -> commit();
        case Ast.Rollback r -> rollback();
        case Ast.SetVar v -> set(v);
        case Ast.ShowVar v -> show(v);
        case Ast.CreateTable ct ->
            inTransaction(t -> SchemaChanges.createTable(engine.catalog(), t, ct));
        case Ast.CreateIndex ci -> {
          outsideBlock("CREATE INDEX");
          yield SchemaChanges.createIndex(engine.catalog(), engine.store(), ci);
        }
        case Ast.DropIndex di -> {
          outsideBlock("DROP INDEX");
          yield SchemaChanges.dropIndex(engine.catalog(), engine.store(), di);
        }
        case Ast.DropTable dt -> {
          outsideBlock("DROP TABLE");
          yield SchemaChanges.dropTable(engine.catalog(), engine.store(), dt);
        }
        case Ast.Explain e -> inTransaction(t -> explain(t, e.statement(), e.analyze(), params));
        case Ast.Analyze a -> inTransaction(t -> analyze(t, a));
        case Ast.QueryStatement q -> inTransaction(t -> runBound(t, s, p, params));
        case Ast.Insert i -> inTransaction(t -> runBound(t, s, p, params));
        case Ast.Update u -> inTransaction(t -> runBound(t, s, p, params));
        case Ast.Delete d -> inTransaction(t -> runBound(t, s, p, params));
      };
    } catch (SqlException e) {
      throw e;
    } catch (RuntimeException e) {
      // A bug: report it, keep the connection.
      fail();
      throw new SqlException(SqlState.INTERNAL_ERROR, "internal error: " + e, e);
    }
  }

  private void outsideBlock(String what) {
    if (state != TxnState.IDLE) {
      fail();
      throw new SqlException(
          SqlState.ACTIVE_SQL_TRANSACTION, what + " cannot run inside a transaction block");
    }
  }

  private Result begin() {
    if (state != TxnState.IDLE) {
      return Result.command("BEGIN", "WARNING: there is already a transaction in progress");
    }
    txn = engine.store().begin();
    state = TxnState.OPEN;
    return Result.command("BEGIN");
  }

  private Result commit() {
    if (state == TxnState.IDLE) {
      return Result.command("COMMIT", "WARNING: there is no transaction in progress");
    }
    KvTxn t = txn;
    boolean failed = state == TxnState.FAILED;
    txn = null;
    state = TxnState.IDLE;
    if (failed) {
      rollbackQuietly(t);
      return Result.command("ROLLBACK");
    }
    try {
      t.commit();
    } catch (SqlException e) {
      rollbackQuietly(t);
      throw e;
    }
    return Result.command("COMMIT");
  }

  private Result rollback() {
    if (state == TxnState.IDLE) {
      return Result.command("ROLLBACK", "WARNING: there is no transaction in progress");
    }
    KvTxn t = txn;
    txn = null;
    state = TxnState.IDLE;
    rollbackQuietly(t);
    return Result.command("ROLLBACK");
  }

  private Result set(Ast.SetVar v) {
    if (!v.name().equals("transaction")) {
      vars.put(v.name().toLowerCase(Locale.ROOT), v.value());
    }
    return Result.command("SET");
  }

  private Result show(Ast.ShowVar v) {
    String value = setting(v.name());
    if (value == null) {
      throw new SqlException(
          SqlState.UNDEFINED_OBJECT, "unrecognized configuration parameter \"" + v.name() + "\"");
    }
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {value});
    return new Result("SHOW", List.of(new Result.Column(v.name(), Type.TEXT)), rows, List.of());
  }

  /** Runs {@code work} in the open transaction, or in a new one (retried) under autocommit. */
  private Result inTransaction(Function<KvTxn, Result> work) {
    if (state == TxnState.OPEN) {
      try {
        return work.apply(txn);
      } catch (RuntimeException e) {
        fail();
        throw e;
      }
    }
    for (int attempt = 1; ; attempt++) {
      KvTxn t = engine.store().begin();
      try {
        Result r = work.apply(t);
        t.commit();
        return r;
      } catch (SqlException e) {
        rollbackQuietly(t);
        if (!e.retryable() || attempt >= AUTOCOMMIT_ATTEMPTS) {
          throw e;
        }
      } catch (RuntimeException e) {
        rollbackQuietly(t);
        throw e;
      }
    }
  }

  /** Marks an open transaction failed (nothing to do outside one). */
  private void fail() {
    if (state == TxnState.OPEN) {
      state = TxnState.FAILED;
    }
  }

  /** Whether statements push work to the ranges ({@code SET distdb_pushdown = on|off}). */
  boolean pushdown() {
    String v = setting("distdb_pushdown");
    return v == null
        || !(v.equalsIgnoreCase("off") || v.equalsIgnoreCase("false") || v.equals("0"));
  }

  private Bound bind(Catalog.Snapshot snapshot, Ast.Statement s, Prepared p) {
    if (p != null && p.bound != null && p.catalogVersion == snapshot.version()) {
      return p.bound;
    }
    Binder binder = new Binder(snapshot, p == null ? List.of() : p.paramTypes);
    Bound bound = binder.bind(s);
    if (p != null) {
      p.bound = bound;
      p.catalogVersion = snapshot.version();
    }
    return bound;
  }

  /** The physical plan a bound statement runs: the query, or the rows a write changes. */
  private Plan physical(Bound bound, Catalog.Snapshot snapshot) {
    Plan logical =
        switch (bound) {
          case Bound.Query q -> q.plan();
          case Bound.Insert i -> i.source();
          case Bound.Update u -> u.source();
          case Bound.Delete d -> d.source();
        };
    return Planner.plan(logical, pushdown(), snapshot);
  }

  private Result runBound(KvTxn t, Ast.Statement s, Prepared p, Object[] params) {
    Catalog.Snapshot snapshot = engine.catalog().load(t);
    Bound bound = bind(snapshot, s, p);
    if (params.length < countParams(p)) {
      throw new SqlException(
          SqlState.PROTOCOL_VIOLATION, "not enough parameters: " + params.length);
    }
    ExecContext ctx = new ExecContext(t, engine.store(), params).pushdown(pushdown());
    return run(bound, physical(bound, snapshot), ctx);
  }

  private Result run(Bound bound, Plan plan, ExecContext ctx) {
    return switch (bound) {
      case Bound.Query q -> {
        List<Object[]> rows = Operators.drain(Operators.build(plan, ctx));
        yield new Result("SELECT " + rows.size(), columnsOf(bound), rows, List.of());
      }
      case Bound.Insert i -> {
        Writes.Outcome o = Writes.insert(i, plan, ctx);
        yield new Result("INSERT 0 " + o.count(), columnsOf(bound), o.returning(), List.of());
      }
      case Bound.Update u -> {
        Writes.Outcome o = Writes.update(u, plan, ctx);
        yield new Result("UPDATE " + o.count(), columnsOf(bound), o.returning(), List.of());
      }
      case Bound.Delete d -> {
        Writes.Outcome o = Writes.delete(d, plan, ctx);
        yield new Result("DELETE " + o.count(), columnsOf(bound), o.returning(), List.of());
      }
    };
  }

  private static int countParams(Prepared p) {
    return p == null || p.paramTypes == null ? 0 : p.paramTypes.size();
  }

  /**
   * {@code EXPLAIN}: the physical plan. {@code EXPLAIN ANALYZE}: the statement run (writes
   * included, as in PostgreSQL), each operator with what it did: rows, time, and for remote scans
   * the requests, nodes, rows read on the ranges and bytes received.
   */
  private Result explain(KvTxn t, Ast.Statement s, boolean analyze, Object[] params) {
    Catalog.Snapshot snapshot = engine.catalog().load(t);
    List<String> lines = new ArrayList<>();
    if (!(s instanceof Ast.QueryStatement
        || s instanceof Ast.Insert
        || s instanceof Ast.Update
        || s instanceof Ast.Delete)) {
      lines.add(s.getClass().getSimpleName().toLowerCase(Locale.ROOT));
    } else {
      Bound bound = new Binder(snapshot, List.of()).bind(s);
      Plan plan = physical(bound, snapshot);
      String head =
          switch (bound) {
            case Bound.Query q -> null;
            case Bound.Insert i -> "insert into " + i.table().name();
            case Bound.Update u -> "update " + u.table().name();
            case Bound.Delete d -> "delete from " + d.table().name();
          };
      List<String> body;
      if (analyze) {
        ExecContext ctx =
            ExecContext.analyzing(t, engine.store(), params, engine.clock()).pushdown(pushdown());
        long started = engine.clock().nanoTime();
        Result r = run(bound, plan, ctx);
        long nanos = engine.clock().nanoTime() - started;
        body = PlanPrinter.render(plan, ctx.opStats(), ctx.remoteStats());
        body = new ArrayList<>(body);
        body.add(String.format(Locale.ROOT, "%s in %.3f ms", r.tag(), nanos / 1e6));
      } else {
        body = PlanPrinter.render(plan);
      }
      if (head == null) {
        lines.addAll(body);
      } else {
        lines.add(head);
        body.forEach(l -> lines.add("  -> " + l));
      }
    }
    List<Object[]> rows = new ArrayList<>();
    for (String l : lines) {
      rows.add(new Object[] {l});
    }
    return new Result(
        "EXPLAIN", List.of(new Result.Column("QUERY PLAN", Type.TEXT)), rows, List.of());
  }

  /** {@code ANALYZE [table]}: statistics for one table, or for all of them. */
  private Result analyze(KvTxn t, Ast.Analyze a) {
    Catalog.Snapshot snapshot = engine.catalog().load(t);
    List<io.distdb.sql.catalog.TableDesc> tables =
        a.table() == null
            ? List.copyOf(snapshot.tables().values())
            : List.of(snapshot.table(a.table()));
    for (io.distdb.sql.catalog.TableDesc d : tables) {
      byte[] start = io.distdb.sql.encoding.RowCodec.tablePrefix(d.id());
      byte[] end = io.distdb.common.Bytes.prefixEnd(start).orElseThrow();
      int ranges = engine.store().rangeCount(start, end);
      Catalog.putStats(t, snapshot, d.id(), io.distdb.sql.stats.Analyzer.analyze(t, d, ranges));
    }
    return Result.command("ANALYZE");
  }

  /** Closes an open transaction, if any (the connection is going away). */
  public void close() {
    if (txn != null) {
      rollbackQuietly(txn);
      txn = null;
    }
    state = TxnState.IDLE;
  }

  static void rollbackQuietly(KvTxn t) {
    try {
      t.rollback();
    } catch (RuntimeException ignored) {
      // the transaction's locks expire anyway
    }
  }
}
