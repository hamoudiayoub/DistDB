package io.distdb.server;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

/**
 * Keys and values on the command line. Input is UTF-8 text where {@code \xNN} stands for any byte
 * and {@code \\} for a backslash. Output is the same notation: readable text stays as it is, and
 * everything else (invalid UTF-8, control characters such as tab and newline) is escaped, so one
 * entry always prints as one line.
 */
final class ByteText {

  private ByteText() {}

  static byte[] parse(String text) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    StringBuilder plain = new StringBuilder();
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c != '\\') {
        plain.append(c);
        continue;
      }
      out.writeBytes(plain.toString().getBytes(StandardCharsets.UTF_8));
      plain.setLength(0);
      if (i + 1 < text.length() && text.charAt(i + 1) == '\\') {
        out.write('\\');
        i++;
      } else if (i + 3 < text.length() && text.charAt(i + 1) == 'x') {
        out.write(HexFormat.fromHexDigits(text, i + 2, i + 4));
        i += 3;
      } else {
        throw new IllegalArgumentException(
            "bad escape at position " + i + " in \"" + text + "\": use \\xNN or \\\\");
      }
    }
    out.writeBytes(plain.toString().getBytes(StandardCharsets.UTF_8));
    return out.toByteArray();
  }

  static String format(byte[] bytes) {
    String text = decodeStrictly(bytes);
    if (text != null && text.codePoints().noneMatch(Character::isISOControl)) {
      return text.replace("\\", "\\\\");
    }
    StringBuilder out = new StringBuilder();
    for (byte b : bytes) {
      int u = b & 0xFF;
      if (u == '\\') {
        out.append("\\\\");
      } else if (u >= 0x20 && u < 0x7F) {
        out.append((char) u);
      } else {
        out.append("\\x").append(HexFormat.of().withUpperCase().toHexDigits((byte) u));
      }
    }
    return out.toString();
  }

  private static String decodeStrictly(byte[] bytes) {
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(bytes))
          .toString();
    } catch (CharacterCodingException e) {
      return null;
    }
  }
}
