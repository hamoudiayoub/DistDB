package io.distdb.sql.catalog;

import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.types.TypeSpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A table's definition, as stored in the catalog. Columns and indexes have ids that never change
 * and are never reused, so stored rows and index entries stay readable across schema changes. The
 * primary index has id 1; its key is the primary key (or the hidden {@code rowid}).
 */
public record TableDesc(
    long id,
    String name,
    List<Column> columns,
    Index primary,
    List<Index> indexes,
    int nextColumnId,
    int nextIndexId) {

  /** The hidden column of a table declared without a primary key. */
  public static final String ROWID = "rowid";

  public static final int PRIMARY_INDEX_ID = 1;

  public TableDesc {
    columns = List.copyOf(columns);
    indexes = List.copyOf(indexes);
    Objects.requireNonNull(primary);
  }

  /**
   * A column; {@code defaultSql} is its default expression's source text, or null.
   *
   * @param hidden not part of {@code SELECT *} (the {@code rowid} of a table without a primary key)
   */
  public record Column(
      int id, String name, TypeSpec type, boolean notNull, boolean hidden, String defaultSql) {}

  public enum IndexState {
    /** Writers maintain it; readers do not use it yet (it is being backfilled). */
    WRITE_ONLY,
    /** Complete: readers may use it. */
    PUBLIC
  }

  /** An index on columns (by column id). The primary index is always PUBLIC and unique. */
  public record Index(
      int id, String name, List<Integer> columnIds, boolean unique, IndexState state) {
    public Index {
      columnIds = List.copyOf(columnIds);
    }

    public Index withState(IndexState s) {
      return new Index(id, name, columnIds, unique, s);
    }
  }

  /** The position in a row of the column with this id. */
  public int position(int columnId) {
    for (int p = 0; p < columns.size(); p++) {
      if (columns.get(p).id() == columnId) {
        return p;
      }
    }
    throw new IllegalStateException("no column " + columnId + " in " + name);
  }

  /** The position of the column named {@code name}, or -1. */
  public int position(String name) {
    for (int p = 0; p < columns.size(); p++) {
      if (columns.get(p).name().equals(name)) {
        return p;
      }
    }
    return -1;
  }

  public int positionOrThrow(String column) {
    int p = position(column);
    if (p < 0) {
      throw new SqlException(
          SqlState.UNDEFINED_COLUMN,
          "column \"" + column + "\" of relation \"" + name + "\" does not exist");
    }
    return p;
  }

  /** Row positions of an index's columns. */
  public int[] positions(Index index) {
    int[] out = new int[index.columnIds().size()];
    for (int k = 0; k < out.length; k++) {
      out[k] = position(index.columnIds().get(k));
    }
    return out;
  }

  public boolean hasRowid() {
    return primary.columnIds().size() == 1
        && columns.get(position(primary.columnIds().get(0))).hidden();
  }

  public Index index(int indexId) {
    if (indexId == PRIMARY_INDEX_ID) {
      return primary;
    }
    for (Index i : indexes) {
      if (i.id() == indexId) {
        return i;
      }
    }
    return null;
  }

  public TableDesc withIndexes(List<Index> newIndexes, int newNextIndexId) {
    return new TableDesc(id, name, columns, primary, newIndexes, nextColumnId, newNextIndexId);
  }

  /** This table with {@code index} replaced (same id) or added. */
  public TableDesc withIndex(Index index) {
    List<Index> out = new ArrayList<>();
    boolean replaced = false;
    for (Index i : indexes) {
      if (i.id() == index.id()) {
        out.add(index);
        replaced = true;
      } else {
        out.add(i);
      }
    }
    if (!replaced) {
      out.add(index);
    }
    return withIndexes(out, Math.max(nextIndexId, index.id() + 1));
  }

  public TableDesc withoutIndex(int indexId) {
    List<Index> out = new ArrayList<>();
    for (Index i : indexes) {
      if (i.id() != indexId) {
        out.add(i);
      }
    }
    return withIndexes(out, nextIndexId);
  }
}
