package io.distdb.shell;

import java.util.regex.Pattern;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

/** Colors SQL as it is typed: keywords, strings, numbers, comments, parameters, meta commands. */
final class SqlHighlighter implements Highlighter {

  private static final AttributedStyle KEYWORD = AttributedStyle.DEFAULT.foreground(75).bold();
  private static final AttributedStyle STRING = AttributedStyle.DEFAULT.foreground(114);
  private static final AttributedStyle NAME = AttributedStyle.DEFAULT.foreground(117);
  private static final AttributedStyle NUMBER = AttributedStyle.DEFAULT.foreground(215);
  private static final AttributedStyle COMMENT = AttributedStyle.DEFAULT.foreground(245).italic();
  private static final AttributedStyle PARAMETER = AttributedStyle.DEFAULT.foreground(221);
  private static final AttributedStyle META = AttributedStyle.DEFAULT.foreground(176).bold();
  private static final AttributedStyle OPEN = AttributedStyle.DEFAULT.foreground(203);

  @Override
  public AttributedString highlight(LineReader reader, String buffer) {
    AttributedStringBuilder b = new AttributedStringBuilder(buffer.length() + 16);
    if (buffer.stripLeading().startsWith("\\")) {
      int space = buffer.indexOf(' ', buffer.indexOf('\\'));
      int end = space < 0 ? buffer.length() : space;
      b.append(buffer.substring(0, end), META).append(buffer.substring(end));
      return b.toAttributedString();
    }
    for (SqlLexer.Token t : SqlLexer.tokens(buffer)) {
      String text = t.text(buffer);
      switch (t.kind()) {
        case KEYWORD -> b.append(text, KEYWORD);
        case STRING -> b.append(text, STRING);
        case QUOTED_NAME -> b.append(text, NAME);
        case NUMBER -> b.append(text, NUMBER);
        case COMMENT -> b.append(text, COMMENT);
        case PARAMETER -> b.append(text, PARAMETER);
        case OPEN -> b.append(text, OPEN);
        default -> b.append(text);
      }
    }
    return b.toAttributedString();
  }

  @Override
  public void setErrorPattern(Pattern errorPattern) {
    // errors are shown after a statement runs, not while it is typed
  }

  @Override
  public void setErrorIndex(int errorIndex) {
    // likewise
  }
}
