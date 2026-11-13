package io.distdb.shell;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

/**
 * Tab completion: meta commands at the start of a line; otherwise keywords (in the case the word
 * was started in), table names, and column names. The shell refreshes the names after any statement
 * that changes the schema.
 */
final class SqlCompleter implements Completer {

  /** Table name -> its column names. */
  private volatile Map<String, List<String>> schema = Map.of();

  void schema(Map<String, List<String>> tables) {
    this.schema = new TreeMap<>(tables);
  }

  @Override
  public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
    String word = line.word();
    boolean first = line.wordIndex() == 0;
    if (word.startsWith("\\") || (first && line.line().stripLeading().startsWith("\\"))) {
      for (Shell.Meta m : Shell.Meta.values()) {
        candidates.add(
            new Candidate(m.command(), m.command(), "commands", m.help(), null, null, true));
      }
      if (!first) {
        tables(candidates);
      }
      return;
    }
    boolean upper = !word.isEmpty() && Character.isUpperCase(word.charAt(0));
    for (String k : SqlLexer.KEYWORDS) {
      String text = upper ? k.toUpperCase(Locale.ROOT) : k;
      candidates.add(new Candidate(text, text, "keywords", null, null, null, true));
    }
    tables(candidates);
    Set<String> columns = new TreeSet<>();
    schema.values().forEach(columns::addAll);
    for (String c : columns) {
      candidates.add(new Candidate(c, c, "columns", null, null, null, true));
    }
  }

  private void tables(List<Candidate> candidates) {
    for (String t : schema.keySet()) {
      candidates.add(new Candidate(t, t, "tables", null, null, null, true));
    }
  }
}
