package io.distdb.sql;

/** The SQLSTATE codes DistDB reports: PostgreSQL's, so clients handle them the same way. */
public final class SqlState {

  public static final String SYNTAX_ERROR = "42601";
  public static final String UNDEFINED_TABLE = "42P01";
  public static final String UNDEFINED_COLUMN = "42703";
  public static final String UNDEFINED_OBJECT = "42704";
  public static final String UNDEFINED_FUNCTION = "42883";
  public static final String DUPLICATE_TABLE = "42P07";
  public static final String DUPLICATE_COLUMN = "42701";
  public static final String DUPLICATE_ALIAS = "42712";
  public static final String AMBIGUOUS_COLUMN = "42702";
  public static final String GROUPING_ERROR = "42803";
  public static final String DATATYPE_MISMATCH = "42804";
  public static final String CANNOT_COERCE = "42846";
  public static final String INVALID_COLUMN_REFERENCE = "42P10";
  public static final String INVALID_TABLE_DEFINITION = "42P16";
  public static final String WRONG_OBJECT_TYPE = "42809";
  public static final String INVALID_TEXT_REPRESENTATION = "22P02";
  public static final String NUMERIC_VALUE_OUT_OF_RANGE = "22003";
  public static final String DIVISION_BY_ZERO = "22012";
  public static final String STRING_DATA_RIGHT_TRUNCATION = "22001";
  public static final String INVALID_PARAMETER_VALUE = "22023";
  public static final String INVALID_ESCAPE_SEQUENCE = "22025";
  public static final String INVALID_ROW_COUNT = "2201W";
  public static final String NOT_NULL_VIOLATION = "23502";
  public static final String UNIQUE_VIOLATION = "23505";
  public static final String CARDINALITY_VIOLATION = "21000";
  public static final String SERIALIZATION_FAILURE = "40001";
  public static final String STATEMENT_COMPLETION_UNKNOWN = "40003";
  public static final String ACTIVE_SQL_TRANSACTION = "25001";
  public static final String NO_ACTIVE_SQL_TRANSACTION = "25P01";
  public static final String IN_FAILED_SQL_TRANSACTION = "25P02";
  public static final String DEPENDENT_OBJECTS_STILL_EXIST = "2BP01";
  public static final String FEATURE_NOT_SUPPORTED = "0A000";
  public static final String PROTOCOL_VIOLATION = "08P01";
  public static final String CONNECTION_FAILURE = "08006";
  public static final String SYSTEM_ERROR = "58000";
  public static final String INTERNAL_ERROR = "XX000";
  public static final String DATA_CORRUPTED = "XX001";

  private SqlState() {}
}
