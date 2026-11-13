package io.distdb.sql.parse;

import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.parse.Ast.Assignment;
import io.distdb.sql.parse.Ast.Between;
import io.distdb.sql.parse.Ast.Binary;
import io.distdb.sql.parse.Ast.Case;
import io.distdb.sql.parse.Ast.Cast;
import io.distdb.sql.parse.Ast.ColumnDef;
import io.distdb.sql.parse.Ast.ColumnRef;
import io.distdb.sql.parse.Ast.CreateIndex;
import io.distdb.sql.parse.Ast.CreateTable;
import io.distdb.sql.parse.Ast.Cte;
import io.distdb.sql.parse.Ast.Default;
import io.distdb.sql.parse.Ast.Delete;
import io.distdb.sql.parse.Ast.Derived;
import io.distdb.sql.parse.Ast.DropIndex;
import io.distdb.sql.parse.Ast.DropTable;
import io.distdb.sql.parse.Ast.Exists;
import io.distdb.sql.parse.Ast.Explain;
import io.distdb.sql.parse.Ast.Expr;
import io.distdb.sql.parse.Ast.ExprItem;
import io.distdb.sql.parse.Ast.Func;
import io.distdb.sql.parse.Ast.InList;
import io.distdb.sql.parse.Ast.InSubquery;
import io.distdb.sql.parse.Ast.Insert;
import io.distdb.sql.parse.Ast.IsBool;
import io.distdb.sql.parse.Ast.IsDistinct;
import io.distdb.sql.parse.Ast.IsNull;
import io.distdb.sql.parse.Ast.Join;
import io.distdb.sql.parse.Ast.JoinKind;
import io.distdb.sql.parse.Ast.Like;
import io.distdb.sql.parse.Ast.Literal;
import io.distdb.sql.parse.Ast.Nested;
import io.distdb.sql.parse.Ast.OrderItem;
import io.distdb.sql.parse.Ast.Param;
import io.distdb.sql.parse.Ast.Query;
import io.distdb.sql.parse.Ast.QueryBody;
import io.distdb.sql.parse.Ast.QueryStatement;
import io.distdb.sql.parse.Ast.ScalarSubquery;
import io.distdb.sql.parse.Ast.Select;
import io.distdb.sql.parse.Ast.SelectItem;
import io.distdb.sql.parse.Ast.SetOp;
import io.distdb.sql.parse.Ast.SetOpKind;
import io.distdb.sql.parse.Ast.SetVar;
import io.distdb.sql.parse.Ast.ShowVar;
import io.distdb.sql.parse.Ast.Star;
import io.distdb.sql.parse.Ast.Statement;
import io.distdb.sql.parse.Ast.TableName;
import io.distdb.sql.parse.Ast.TableRef;
import io.distdb.sql.parse.Ast.Unary;
import io.distdb.sql.parse.Ast.Update;
import io.distdb.sql.parse.Ast.Values;
import io.distdb.sql.parse.Ast.When;
import io.distdb.sql.parse.Lexer.Kind;
import io.distdb.sql.parse.Lexer.Token;
import io.distdb.sql.types.Type;
import io.distdb.sql.types.TypeSpec;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A hand-written recursive-descent parser for DistDB's SQL, with Pratt parsing (binding powers) for
 * expressions. Operator precedence follows PostgreSQL's, lowest first:
 *
 * <pre>
 *   OR  &lt;  AND  &lt;  NOT  &lt;  IS  &lt;  = &lt;&gt; &lt; &gt; &lt;= &gt;=  &lt;  BETWEEN IN LIKE
 *   &lt;  ||  &lt;  + -  &lt;  * / %  &lt;  ^  &lt;  unary -  &lt;  ::
 * </pre>
 */
public final class Parser {

  /** Words that cannot be a bare column name or alias. */
  private static final Set<String> RESERVED =
      Set.of(
          "all",
          "and",
          "any",
          "as",
          "asc",
          "between",
          "both",
          "case",
          "cast",
          "check",
          "collate",
          "constraint",
          "create",
          "cross",
          "default",
          "desc",
          "distinct",
          "do",
          "else",
          "end",
          "except",
          "false",
          "fetch",
          "for",
          "foreign",
          "from",
          "full",
          "group",
          "having",
          "ilike",
          "in",
          "inner",
          "intersect",
          "into",
          "is",
          "isnull",
          "join",
          "leading",
          "left",
          "like",
          "limit",
          "natural",
          "not",
          "notnull",
          "null",
          "offset",
          "on",
          "only",
          "or",
          "order",
          "outer",
          "primary",
          "references",
          "returning",
          "right",
          "select",
          "set",
          "some",
          "table",
          "then",
          "to",
          "trailing",
          "true",
          "union",
          "unique",
          "using",
          "values",
          "when",
          "where",
          "window",
          "with");

  private static final int BP_OR = 10;
  private static final int BP_AND = 20;
  private static final int BP_NOT = 30;
  private static final int BP_IS = 40;
  private static final int BP_COMPARE = 50;
  private static final int BP_RANGE = 60; // BETWEEN, IN, LIKE
  private static final int BP_OTHER = 70; // ||
  private static final int BP_ADD = 80;
  private static final int BP_MUL = 90;
  private static final int BP_POW = 100;
  private static final int BP_UNARY = 110;
  private static final int BP_CAST = 120;

  private final String sql;
  private final List<Token> tokens;
  private int i;

  private Parser(String sql) {
    this.sql = sql;
    this.tokens = Lexer.tokenize(sql);
  }

  /** Parses a string of statements separated by semicolons; empty statements are skipped. */
  public static List<Statement> parse(String sql) {
    Parser p = new Parser(sql);
    List<Statement> out = new ArrayList<>();
    while (true) {
      while (p.acceptOp(";")) {
        // empty statement
      }
      if (p.peek().kind() == Kind.EOF) {
        return out;
      }
      out.add(p.statement());
      if (!p.acceptOp(";") && p.peek().kind() != Kind.EOF) {
        throw p.unexpected();
      }
    }
  }

  /** Parses exactly one statement. */
  public static Statement parseOne(String sql) {
    List<Statement> all = parse(sql);
    if (all.size() != 1) {
      throw new SqlException(
          SqlState.SYNTAX_ERROR,
          all.isEmpty()
              ? "empty statement"
              : "cannot insert multiple commands into a prepared statement");
    }
    return all.get(0);
  }

  /** Parses one expression, e.g. a column default stored in the catalog. */
  public static Expr parseExpression(String sql) {
    Parser p = new Parser(sql);
    Expr e = p.expr(0);
    if (p.peek().kind() != Kind.EOF) {
      throw p.unexpected();
    }
    return e;
  }

  // -----------------------------------------------------------------------------------------------
  // Statements

  private Statement statement() {
    Token t = peek();
    if (t.is("select") || t.is("values") || t.is("with") || t.isOp("(")) {
      return new QueryStatement(query());
    }
    if (t.kind() != Kind.WORD) {
      throw unexpected();
    }
    return switch (t.text()) {
      case "insert" -> insert();
      case "update" -> update();
      case "delete" -> delete();
      case "create" -> create();
      case "drop" -> drop();
      case "begin", "start" -> begin();
      case "commit", "end" -> {
        advance();
        if (!accept("transaction")) {
          accept("work");
        }
        yield new Ast.Commit();
      }
      case "rollback", "abort" -> {
        advance();
        if (!accept("transaction")) {
          accept("work");
        }
        yield new Ast.Rollback();
      }
      case "set" -> set();
      case "show" -> show();
      case "explain" -> {
        advance();
        boolean analyze = accept("analyze") || accept("analyse");
        yield new Explain(statement(), analyze);
      }
      case "analyze", "analyse" -> {
        advance();
        yield new Ast.Analyze(peek().kind() == Kind.EOF || peek().isOp(";") ? null : identifier());
      }
      default -> throw unexpected();
    };
  }

  private Statement begin() {
    if (accept("start")) {
      expect("transaction");
    } else {
      expect("begin");
      if (!accept("transaction")) {
        accept("work");
      }
    }
    transactionModes();
    return new Ast.Begin();
  }

  /**
   * {@code ISOLATION LEVEL ...}, {@code READ WRITE}, ... DistDB gives snapshot isolation, which is
   * PostgreSQL's REPEATABLE READ and stronger than READ COMMITTED; SERIALIZABLE is refused rather
   * than silently weakened.
   */
  private void transactionModes() {
    while (true) {
      if (accept("isolation")) {
        expect("level");
        if (accept("serializable")) {
          throw unsupported(
              "SERIALIZABLE isolation (DistDB provides snapshot isolation: REPEATABLE READ)");
        }
        if (accept("repeatable")) {
          expect("read");
        } else if (accept("read")) {
          if (!accept("committed")) {
            expect("uncommitted");
          }
        } else {
          throw unexpected();
        }
      } else if (accept("read")) {
        if (!accept("write")) {
          expect("only");
        }
      } else if (accept("not")) {
        expect("deferrable");
      } else if (!accept("deferrable")) {
        return;
      }
      acceptOp(",");
    }
  }

  private Statement set() {
    expect("set");
    if (!accept("session")) {
      accept("local");
    }
    if (accept("transaction")
        || (accept("characteristics") && expect("as") && expect("transaction"))) {
      transactionModes();
      return new SetVar("transaction", "");
    }
    String name;
    if (accept("time")) {
      expect("zone");
      name = "timezone";
    } else {
      name = identifier();
      while (acceptOp(".")) {
        name = name + "." + identifier();
      }
      if (!acceptOp("=")) {
        expect("to");
      }
    }
    int from = peek().start();
    int to = from;
    while (peek().kind() != Kind.EOF && !peek().isOp(";")) {
      to = advance().end();
    }
    String value = sql.substring(from, to).strip();
    if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
      value = value.substring(1, value.length() - 1).replace("''", "'");
    }
    return new SetVar(name, value);
  }

  private Statement show() {
    expect("show");
    if (accept("tables")) {
      return new Ast.ShowCatalog(Ast.ShowCatalog.What.TABLES, null);
    }
    if (accept("nodes")) {
      return new Ast.ShowCatalog(Ast.ShowCatalog.What.NODES, null);
    }
    if (accept("columns")) {
      expect("from");
      return new Ast.ShowCatalog(Ast.ShowCatalog.What.COLUMNS, identifier());
    }
    if (accept("indexes") || accept("index")) {
      expect("from");
      return new Ast.ShowCatalog(Ast.ShowCatalog.What.INDEXES, identifier());
    }
    if (accept("statistics")) {
      expect("for");
      accept("table");
      return new Ast.ShowCatalog(Ast.ShowCatalog.What.STATISTICS, identifier());
    }
    if (accept("ranges")) {
      String table = null;
      if (accept("from")) {
        expect("table");
        table = identifier();
      }
      return new Ast.ShowCatalog(Ast.ShowCatalog.What.RANGES, table);
    }
    if (accept("transaction")) {
      expect("isolation");
      expect("level");
      return new ShowVar("transaction_isolation");
    }
    if (accept("time")) {
      expect("zone");
      return new ShowVar("timezone");
    }
    String name = identifier();
    while (acceptOp(".")) {
      name = name + "." + identifier();
    }
    return new ShowVar(name);
  }

  private Statement create() {
    expect("create");
    boolean unique = accept("unique");
    if (accept("index")) {
      return createIndex(unique);
    }
    if (unique) {
      throw unexpected();
    }
    expect("table");
    boolean ifNotExists = false;
    if (accept("if")) {
      expect("not");
      expect("exists");
      ifNotExists = true;
    }
    String name = identifier();
    expectOp("(");
    List<ColumnDef> columns = new ArrayList<>();
    List<String> primaryKey = null;
    List<List<String>> uniques = new ArrayList<>();
    do {
      if (accept("constraint")) {
        identifier(); // the constraint's name: not kept
      }
      if (accept("primary")) {
        expect("key");
        if (primaryKey != null) {
          throw new SqlException(
              SqlState.INVALID_TABLE_DEFINITION,
              "multiple primary keys for table \"" + name + "\" are not allowed");
        }
        primaryKey = identifierList();
      } else if (accept("unique")) {
        uniques.add(identifierList());
      } else if (peek().is("check") || peek().is("foreign") || peek().is("exclude")) {
        throw unsupported(peek().text().toUpperCase(java.util.Locale.ROOT) + " constraints");
      } else {
        columns.add(columnDef());
      }
    } while (acceptOp(","));
    expectOp(")");
    return new CreateTable(name, ifNotExists, columns, primaryKey, uniques);
  }

  private ColumnDef columnDef() {
    String name = identifier();
    TypeSpec type = typeName();
    boolean notNull = false;
    boolean primaryKey = false;
    boolean unique = false;
    Expr defaultExpr = null;
    String defaultSql = null;
    if (pendingSerial) {
      pendingSerial = false;
      notNull = true;
      defaultSql = "unique_rowid()";
      defaultExpr = parseExpression(defaultSql);
    }
    while (true) {
      if (accept("constraint")) {
        identifier();
      }
      if (accept("not")) {
        expect("null");
        notNull = true;
      } else if (accept("null")) {
        // nullable: the default
      } else if (accept("primary")) {
        expect("key");
        primaryKey = true;
      } else if (accept("unique")) {
        unique = true;
      } else if (accept("default")) {
        int from = peek().start();
        defaultExpr = expr(BP_IS);
        defaultSql = sql.substring(from, tokens.get(i - 1).end());
      } else if (peek().is("references") || peek().is("check") || peek().is("collate")) {
        throw unsupported(peek().text().toUpperCase(java.util.Locale.ROOT) + " on a column");
      } else {
        break;
      }
    }
    return new ColumnDef(name, type, notNull, primaryKey, unique, defaultExpr, defaultSql);
  }

  private boolean pendingSerial;

  /** A type name, e.g. {@code BIGINT}, {@code VARCHAR(20)}, {@code DOUBLE PRECISION}. */
  private TypeSpec typeName() {
    String word = identifier();
    return switch (word) {
      case "int", "integer", "int2", "int4", "int8", "bigint", "smallint" -> TypeSpec.of(Type.INT);
      case "serial", "bigserial", "serial4", "serial8", "smallserial" -> {
        pendingSerial = true;
        yield TypeSpec.of(Type.INT);
      }
      case "bool", "boolean" -> TypeSpec.of(Type.BOOL);
      case "real", "float4", "float8" -> TypeSpec.of(Type.FLOAT);
      case "float" -> {
        if (acceptOp("(")) {
          integerLiteral();
          expectOp(")");
        }
        yield TypeSpec.of(Type.FLOAT);
      }
      case "double" -> {
        expect("precision");
        yield TypeSpec.of(Type.FLOAT);
      }
      case "numeric", "decimal", "dec" -> {
        if (acceptOp("(")) {
          int precision = integerLiteral();
          int scale = 0;
          if (acceptOp(",")) {
            scale = integerLiteral();
          }
          expectOp(")");
          if (precision < 1 || precision > 1000 || scale < 0 || scale > precision) {
            throw new SqlException(
                SqlState.INVALID_PARAMETER_VALUE,
                "NUMERIC precision " + precision + " and scale " + scale + " are not valid");
          }
          yield TypeSpec.numeric(precision, scale);
        }
        yield TypeSpec.of(Type.NUMERIC);
      }
      case "text", "string" -> TypeSpec.of(Type.TEXT);
      case "varchar", "char", "character", "bpchar" -> {
        if (word.equals("character")) {
          accept("varying");
        }
        if (acceptOp("(")) {
          int length = integerLiteral();
          expectOp(")");
          if (length < 1) {
            throw new SqlException(
                SqlState.INVALID_PARAMETER_VALUE, "length for type varchar must be at least 1");
          }
          yield TypeSpec.varchar(length);
        }
        yield TypeSpec.of(Type.TEXT);
      }
      default ->
          throw new SqlException(SqlState.UNDEFINED_OBJECT, "type \"" + word + "\" does not exist");
    };
  }

  private int integerLiteral() {
    Token t = advance();
    if (t.kind() != Kind.INTEGER) {
      throw unexpected(t);
    }
    return Integer.parseInt(t.text());
  }

  private Statement createIndex(boolean unique) {
    boolean ifNotExists = false;
    if (accept("if")) {
      expect("not");
      expect("exists");
      ifNotExists = true;
    }
    String name = null;
    if (!peek().is("on")) {
      name = identifier();
    }
    expect("on");
    String table = identifier();
    if (accept("using")) {
      identifier(); // the access method: DistDB has one kind of index
    }
    expectOp("(");
    List<String> columns = new ArrayList<>();
    do {
      columns.add(identifier());
      if (accept("desc")) {
        throw unsupported("descending index columns");
      }
      accept("asc");
    } while (acceptOp(","));
    expectOp(")");
    return new CreateIndex(name, table, columns, unique, ifNotExists);
  }

  private Statement drop() {
    expect("drop");
    boolean index = accept("index");
    if (!index) {
      expect("table");
    }
    boolean ifExists = false;
    if (accept("if")) {
      expect("exists");
      ifExists = true;
    }
    String name = identifier();
    if (peek().is("cascade")) {
      throw unsupported("DROP ... CASCADE");
    }
    accept("restrict");
    return index ? new DropIndex(name, ifExists) : new DropTable(name, ifExists);
  }

  private Statement insert() {
    expect("insert");
    expect("into");
    String table = identifier();
    List<String> columns = null;
    if (peek().isOp("(") && !isQueryStart(1)) {
      columns = identifierList();
    }
    Query source;
    if (accept("default")) {
      expect("values");
      source = new Query(List.of(), new Values(List.of(List.of())), List.of(), null, null);
    } else {
      source = query();
    }
    boolean doNothing = false;
    if (accept("on")) {
      expect("conflict");
      if (peek().isOp("(")) {
        identifierList();
      }
      expect("do");
      if (peek().is("update")) {
        throw unsupported("ON CONFLICT DO UPDATE");
      }
      expect("nothing");
      doNothing = true;
    }
    return new Insert(table, columns, source, doNothing, returning());
  }

  private List<SelectItem> returning() {
    if (!accept("returning")) {
      return List.of();
    }
    return selectItems();
  }

  private Statement update() {
    expect("update");
    String table = identifier();
    String alias = alias();
    expect("set");
    List<Assignment> set = new ArrayList<>();
    do {
      String column = identifier();
      expectOp("=");
      set.add(new Assignment(column, expr(0)));
    } while (acceptOp(","));
    if (peek().is("from")) {
      throw unsupported("UPDATE ... FROM");
    }
    Expr where = accept("where") ? expr(0) : null;
    return new Update(table, alias, set, where, returning());
  }

  private Statement delete() {
    expect("delete");
    expect("from");
    String table = identifier();
    String alias = alias();
    if (peek().is("using")) {
      throw unsupported("DELETE ... USING");
    }
    Expr where = accept("where") ? expr(0) : null;
    return new Delete(table, alias, where, returning());
  }

  // -----------------------------------------------------------------------------------------------
  // Queries

  private Query query() {
    List<Cte> with = new ArrayList<>();
    if (accept("with")) {
      if (peek().is("recursive")) {
        throw unsupported("WITH RECURSIVE");
      }
      do {
        String name = identifier();
        List<String> columns = peek().isOp("(") ? identifierList() : List.of();
        expect("as");
        expectOp("(");
        Query q = query();
        expectOp(")");
        with.add(new Cte(name, columns, q));
      } while (acceptOp(","));
    }
    QueryBody body = setExpression();
    List<OrderItem> orderBy = List.of();
    if (accept("order")) {
      expect("by");
      orderBy = new ArrayList<>();
      do {
        Expr e = expr(0);
        boolean desc = false;
        if (accept("desc")) {
          desc = true;
        } else {
          accept("asc");
        }
        Boolean nullsFirst = null;
        if (accept("nulls")) {
          if (accept("first")) {
            nullsFirst = true;
          } else {
            expect("last");
            nullsFirst = false;
          }
        }
        orderBy.add(new OrderItem(e, desc, nullsFirst));
      } while (acceptOp(","));
    }
    Expr limit = null;
    Expr offset = null;
    while (true) {
      if (accept("limit")) {
        limit = accept("all") ? null : expr(0);
      } else if (accept("offset")) {
        offset = expr(0);
        if (!accept("rows")) {
          accept("row");
        }
      } else if (accept("fetch")) {
        if (!accept("first")) {
          expect("next");
        }
        limit = peek().is("row") || peek().is("rows") ? new Literal(1L, Type.INT) : expr(0);
        if (!accept("rows")) {
          expect("row");
        }
        expect("only");
      } else {
        break;
      }
    }
    return new Query(with, body, orderBy, limit, offset);
  }

  private QueryBody setExpression() {
    QueryBody left = setTerm();
    while (peek().is("union") || peek().is("except")) {
      SetOpKind kind = advance().text().equals("union") ? SetOpKind.UNION : SetOpKind.EXCEPT;
      boolean all = accept("all");
      if (!all) {
        accept("distinct");
      }
      left = new SetOp(kind, all, left, setTerm());
    }
    return left;
  }

  private QueryBody setTerm() {
    QueryBody left = setPrimary();
    while (accept("intersect")) {
      boolean all = accept("all");
      if (!all) {
        accept("distinct");
      }
      left = new SetOp(SetOpKind.INTERSECT, all, left, setPrimary());
    }
    return left;
  }

  private QueryBody setPrimary() {
    if (peek().is("select")) {
      return select();
    }
    if (peek().is("values")) {
      return values();
    }
    if (acceptOp("(")) {
      Query q = query();
      expectOp(")");
      if (q.with().isEmpty() && q.orderBy().isEmpty() && q.limit() == null && q.offset() == null) {
        return q.body();
      }
      return new Nested(q);
    }
    throw unexpected();
  }

  private Values values() {
    expect("values");
    List<List<Expr>> rows = new ArrayList<>();
    do {
      expectOp("(");
      List<Expr> row = new ArrayList<>();
      do {
        row.add(expr(0));
      } while (acceptOp(","));
      expectOp(")");
      rows.add(row);
    } while (acceptOp(","));
    return new Values(rows);
  }

  private Select select() {
    expect("select");
    boolean distinct = false;
    if (accept("distinct")) {
      if (peek().is("on")) {
        throw unsupported("SELECT DISTINCT ON");
      }
      distinct = true;
    } else {
      accept("all");
    }
    List<SelectItem> items = selectItems();
    List<TableRef> from = new ArrayList<>();
    if (accept("from")) {
      do {
        from.add(tableRef());
      } while (acceptOp(","));
    }
    Expr where = accept("where") ? expr(0) : null;
    List<Expr> groupBy = new ArrayList<>();
    if (accept("group")) {
      expect("by");
      do {
        groupBy.add(expr(0));
      } while (acceptOp(","));
    }
    Expr having = accept("having") ? expr(0) : null;
    if (peek().is("window")) {
      throw unsupported("window functions");
    }
    return new Select(distinct, items, from, where, groupBy, having);
  }

  private List<SelectItem> selectItems() {
    List<SelectItem> items = new ArrayList<>();
    do {
      if (acceptOp("*")) {
        items.add(new Star(null));
      } else if ((peek().kind() == Kind.WORD || peek().kind() == Kind.QUOTED)
          && peek(1).isOp(".")
          && peek(2).isOp("*")) {
        String q = identifier();
        advance();
        advance();
        items.add(new Star(q));
      } else {
        Expr e = expr(0);
        String alias = null;
        if (accept("as")) {
          alias = anyIdentifier();
        } else if (isBareAlias(peek())) {
          alias = identifier();
        }
        items.add(new ExprItem(e, alias));
      }
    } while (acceptOp(","));
    return items;
  }

  private TableRef tableRef() {
    TableRef left = tablePrimary();
    while (true) {
      JoinKind kind;
      if (accept("cross")) {
        expect("join");
        left = new Join(JoinKind.CROSS, left, tablePrimary(), null, List.of());
        continue;
      }
      if (peek().is("natural")) {
        throw unsupported("NATURAL JOIN");
      }
      if (accept("join")) {
        kind = JoinKind.INNER;
      } else if (accept("inner")) {
        expect("join");
        kind = JoinKind.INNER;
      } else if (peek().is("left") || peek().is("right") || peek().is("full")) {
        String w = advance().text();
        accept("outer");
        expect("join");
        kind =
            switch (w) {
              case "left" -> JoinKind.LEFT;
              case "right" -> JoinKind.RIGHT;
              default -> JoinKind.FULL;
            };
      } else {
        return left;
      }
      TableRef right = tablePrimary();
      if (accept("on")) {
        left = new Join(kind, left, right, expr(0), List.of());
      } else if (accept("using")) {
        left = new Join(kind, left, right, null, identifierList());
      } else {
        throw new SqlException(SqlState.SYNTAX_ERROR, "a JOIN needs ON or USING");
      }
    }
  }

  private TableRef tablePrimary() {
    if (acceptOp("(")) {
      if (isQueryStart(0)) {
        Query q = query();
        expectOp(")");
        accept("as");
        if (!isBareAlias(peek()) && peek().kind() != Kind.QUOTED) {
          throw new SqlException(SqlState.SYNTAX_ERROR, "subquery in FROM must have an alias");
        }
        String alias = identifier();
        List<String> columns = peek().isOp("(") ? identifierList() : List.of();
        return new Derived(q, alias, columns);
      }
      TableRef inner = tableRef();
      expectOp(")");
      return inner;
    }
    String name = identifier();
    return new TableName(name, alias());
  }

  private String alias() {
    if (accept("as")) {
      return identifier();
    }
    if (isBareAlias(peek())) {
      return identifier();
    }
    return null;
  }

  private boolean isBareAlias(Token t) {
    return (t.kind() == Kind.WORD && !RESERVED.contains(t.text())) || t.kind() == Kind.QUOTED;
  }

  private boolean isQueryStart(int ahead) {
    Token t = peek(ahead);
    return t.is("select")
        || t.is("values")
        || t.is("with")
        || (t.isOp("(") && isQueryStart(ahead + 1));
  }

  // -----------------------------------------------------------------------------------------------
  // Expressions

  private Expr expr(int minBp) {
    Expr left = prefix();
    while (true) {
      Token t = peek();
      int bp = infixPower(t);
      if (bp <= minBp) {
        return left;
      }
      left = infix(left, bp);
    }
  }

  private int infixPower(Token t) {
    if (t.kind() == Kind.OP) {
      return switch (t.text()) {
        case "=", "<>", "<", "<=", ">", ">=" -> BP_COMPARE;
        case "||" -> BP_OTHER;
        case "+", "-" -> BP_ADD;
        case "*", "/", "%" -> BP_MUL;
        case "^" -> BP_POW;
        case "::" -> BP_CAST;
        default -> 0;
      };
    }
    if (t.kind() != Kind.WORD) {
      return 0;
    }
    return switch (t.text()) {
      case "or" -> BP_OR;
      case "and" -> BP_AND;
      case "is", "isnull", "notnull" -> BP_IS;
      case "between", "in", "like", "ilike" -> BP_RANGE;
      case "not" -> {
        Token n = peek(1);
        yield n.is("between") || n.is("in") || n.is("like") || n.is("ilike") ? BP_RANGE : 0;
      }
      default -> 0;
    };
  }

  private Expr infix(Expr left, int bp) {
    Token t = advance();
    if (t.kind() == Kind.OP) {
      if (t.text().equals("::")) {
        return new Cast(left, typeName());
      }
      Expr right = expr(bp);
      return new Binary(t.text(), left, right);
    }
    switch (t.text()) {
      case "or", "and" -> {
        return new Binary(t.text().toUpperCase(java.util.Locale.ROOT), left, expr(bp));
      }
      case "isnull" -> {
        return new IsNull(left, false);
      }
      case "notnull" -> {
        return new IsNull(left, true);
      }
      case "is" -> {
        boolean not = accept("not");
        if (accept("null")) {
          return new IsNull(left, not);
        }
        if (accept("true")) {
          return new IsBool(left, true, not);
        }
        if (accept("false")) {
          return new IsBool(left, false, not);
        }
        if (accept("unknown")) {
          return new IsNull(left, not);
        }
        expect("distinct");
        expect("from");
        return new IsDistinct(left, expr(BP_IS), not);
      }
      default -> {
        boolean not = false;
        Token op = t;
        if (t.is("not")) {
          not = true;
          op = advance();
        }
        return switch (op.text()) {
          case "between" -> {
            if (peek().is("symmetric")) {
              throw unsupported("BETWEEN SYMMETRIC");
            }
            Expr low = expr(BP_RANGE);
            expect("and");
            Expr high = expr(BP_RANGE);
            yield new Between(left, low, high, not);
          }
          case "in" -> {
            expectOp("(");
            if (isQueryStart(0)) {
              Query q = query();
              expectOp(")");
              yield new InSubquery(left, q, not);
            }
            List<Expr> list = new ArrayList<>();
            do {
              list.add(expr(0));
            } while (acceptOp(","));
            expectOp(")");
            yield new InList(left, list, not);
          }
          case "like", "ilike" -> {
            Expr pattern = expr(BP_RANGE);
            Expr escape = accept("escape") ? expr(BP_RANGE) : null;
            yield new Like(left, pattern, escape, not, op.text().equals("ilike"));
          }
          default -> throw unexpected(op);
        };
      }
    }
  }

  private Expr prefix() {
    Token t = advance();
    switch (t.kind()) {
      case INTEGER -> {
        return integer(t.text(), false);
      }
      case DECIMAL -> {
        return new Literal(
            io.distdb.sql.types.Values.normalizeScale(new BigDecimal(t.text())), Type.NUMERIC);
      }
      case STRING -> {
        return new Literal(t.text(), Type.UNKNOWN);
      }
      case PARAM -> {
        int index = Integer.parseInt(t.text());
        if (index < 1) {
          throw unexpected(t);
        }
        return new Param(index);
      }
      case OP -> {
        switch (t.text()) {
          case "(" -> {
            if (isQueryStart(0)) {
              Query q = query();
              expectOp(")");
              return new ScalarSubquery(q);
            }
            Expr e = expr(0);
            if (peek().isOp(",")) {
              throw unsupported("row constructors");
            }
            expectOp(")");
            return e;
          }
          case "-" -> {
            if (peek().kind() == Kind.INTEGER && infixPower(peek(1)) < BP_CAST) {
              return integer(advance().text(), true);
            }
            if (peek().kind() == Kind.DECIMAL && infixPower(peek(1)) < BP_CAST) {
              return new Literal(
                  io.distdb.sql.types.Values.normalizeScale(
                      new BigDecimal(advance().text()).negate()),
                  Type.NUMERIC);
            }
            return new Unary("-", expr(BP_UNARY));
          }
          case "+" -> {
            return new Unary("+", expr(BP_UNARY));
          }
          default -> throw unexpected(t);
        }
      }
      case QUOTED -> {
        return name(t.text());
      }
      case WORD -> {
        return word(t);
      }
      case EOF -> throw unexpected(t);
    }
    throw unexpected(t);
  }

  private Expr word(Token t) {
    switch (t.text()) {
      case "null" -> {
        return new Literal(null, Type.UNKNOWN);
      }
      case "true" -> {
        return new Literal(true, Type.BOOL);
      }
      case "false" -> {
        return new Literal(false, Type.BOOL);
      }
      case "not" -> {
        return new Unary("NOT", expr(BP_NOT));
      }
      case "default" -> {
        return new Default();
      }
      case "case" -> {
        return caseExpr();
      }
      case "cast" -> {
        expectOp("(");
        Expr e = expr(0);
        expect("as");
        TypeSpec type = typeName();
        expectOp(")");
        return new Cast(e, type);
      }
      case "exists" -> {
        expectOp("(");
        Query q = query();
        expectOp(")");
        return new Exists(q);
      }
      case "interval", "date", "timestamp", "time" -> {
        if (peek().kind() == Kind.STRING) {
          throw unsupported("dates and times");
        }
      }
      default -> {
        // a column or a function
      }
    }
    if (peek().isOp("(")) {
      return function(t.text());
    }
    if (RESERVED.contains(t.text())) {
      throw unexpected(t);
    }
    return name(t.text());
  }

  private Expr name(String first) {
    if (acceptOp(".")) {
      String second = anyIdentifier();
      if (peek().isOp(".")) {
        throw unsupported("schema-qualified names");
      }
      return new ColumnRef(first, second);
    }
    return new ColumnRef(null, first);
  }

  private Expr function(String name) {
    expectOp("(");
    if (acceptOp("*")) {
      expectOp(")");
      return new Func(name, List.of(), false, true);
    }
    boolean distinct = accept("distinct");
    if (!distinct) {
      accept("all");
    }
    List<Expr> args = new ArrayList<>();
    if (!peek().isOp(")")) {
      // In position(sub IN s), the IN is syntax, not the IN operator.
      args.add(expr(name.equals("position") ? BP_RANGE : 0));
      if (name.equals("substring") && (peek().is("from") || peek().is("for"))) {
        // substring(s FROM start [FOR length])
        Expr start = accept("from") ? expr(0) : new Literal(1L, Type.INT);
        args.add(start);
        if (accept("for")) {
          args.add(expr(0));
        }
      } else if (name.equals("position") && accept("in")) {
        // position(sub IN s) is strpos(s, sub)
        Expr s = expr(0);
        expectOp(")");
        return new Func("strpos", List.of(s, args.get(0)), false, false);
      } else {
        while (acceptOp(",")) {
          args.add(expr(0));
        }
      }
    }
    if (peek().is("order")) {
      throw unsupported("ORDER BY inside an aggregate");
    }
    expectOp(")");
    if (peek().is("over") || peek().is("filter") || peek().is("within")) {
      throw unsupported("window functions and aggregate FILTER");
    }
    return new Func(name, args, distinct, false);
  }

  private Expr caseExpr() {
    Expr operand = peek().is("when") ? null : expr(0);
    List<When> whens = new ArrayList<>();
    while (accept("when")) {
      Expr condition = expr(0);
      expect("then");
      whens.add(new When(condition, expr(0)));
    }
    if (whens.isEmpty()) {
      throw unexpected();
    }
    Expr otherwise = accept("else") ? expr(0) : null;
    expect("end");
    return new Case(operand, whens, otherwise);
  }

  /** An integer literal: INT, or NUMERIC when it does not fit in 64 bits (as in PostgreSQL). */
  private static Literal integer(String digits, boolean negative) {
    String text = negative ? "-" + digits : digits;
    try {
      return new Literal(Long.parseLong(text), Type.INT);
    } catch (NumberFormatException e) {
      return new Literal(new BigDecimal(text), Type.NUMERIC);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Tokens

  private Token peek() {
    return tokens.get(i);
  }

  private Token peek(int ahead) {
    return tokens.get(Math.min(i + ahead, tokens.size() - 1));
  }

  private Token advance() {
    Token t = tokens.get(i);
    if (t.kind() != Kind.EOF) {
      i++;
    }
    return t;
  }

  private boolean accept(String word) {
    if (peek().is(word)) {
      i++;
      return true;
    }
    return false;
  }

  private boolean expect(String word) {
    if (!accept(word)) {
      throw unexpected();
    }
    return true;
  }

  private boolean acceptOp(String op) {
    if (peek().isOp(op)) {
      i++;
      return true;
    }
    return false;
  }

  private void expectOp(String op) {
    if (!acceptOp(op)) {
      throw unexpected();
    }
  }

  /** A name: an unreserved word, or a quoted identifier. */
  private String identifier() {
    Token t = peek();
    if (t.kind() == Kind.QUOTED || (t.kind() == Kind.WORD && !RESERVED.contains(t.text()))) {
      i++;
      return t.text();
    }
    throw unexpected();
  }

  /** Any word, reserved or not (after {@code AS} or a dot). */
  private String anyIdentifier() {
    Token t = peek();
    if (t.kind() == Kind.QUOTED || t.kind() == Kind.WORD) {
      i++;
      return t.text();
    }
    throw unexpected();
  }

  private List<String> identifierList() {
    expectOp("(");
    List<String> names = new ArrayList<>();
    do {
      names.add(identifier());
    } while (acceptOp(","));
    expectOp(")");
    return names;
  }

  private SqlException unexpected() {
    return unexpected(peek());
  }

  private SqlException unexpected(Token t) {
    if (t.kind() == Kind.EOF) {
      return Lexer.error("syntax error at end of input", t.start());
    }
    return Lexer.error(
        "syntax error at or near \"" + sql.substring(t.start(), t.end()) + "\"", t.start());
  }

  private static SqlException unsupported(String what) {
    return new SqlException(SqlState.FEATURE_NOT_SUPPORTED, what + " is not supported");
  }
}
