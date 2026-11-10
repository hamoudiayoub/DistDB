package io.distdb.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;

class CliTest {

  @Property
  void anyBytesSurviveFormattingAndParsing(@ForAll byte[] bytes) {
    assertThat(ByteText.parse(ByteText.format(bytes))).isEqualTo(bytes);
  }

  @Property
  void oneEntryAlwaysPrintsAsOneLine(@ForAll byte[] bytes) {
    assertThat(ByteText.format(bytes)).doesNotContain("\n", "\r", "\t");
  }

  @Test
  void textIsReadableAndEscapesAreExplicit() {
    assertThat(ByteText.parse("plain")).isEqualTo("plain".getBytes(StandardCharsets.UTF_8));
    assertThat(ByteText.parse("a\\x00b")).containsExactly('a', 0, 'b');
    assertThat(ByteText.parse("back\\\\slash"))
        .isEqualTo("back\\slash".getBytes(StandardCharsets.UTF_8));
    assertThat(ByteText.format("café".getBytes(StandardCharsets.UTF_8))).isEqualTo("café");
    assertThat(ByteText.format(new byte[] {'a', '\t', (byte) 0xFF})).isEqualTo("a\\x09\\xFF");
    assertThatThrownBy(() -> ByteText.parse("bad\\q")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ByteText.parse("short\\x4"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ByteText.parse("\\xZZ")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void durationsAndAddressesParse() throws Exception {
    assertThat(Args.duration("250ms")).isEqualTo(Duration.ofMillis(250));
    assertThat(Args.duration("5s")).isEqualTo(Duration.ofSeconds(5));
    assertThat(Args.duration("2m")).isEqualTo(Duration.ofMinutes(2));
    assertThat(Args.duration("3")).isEqualTo(Duration.ofSeconds(3));
    assertThat(Args.hostPort("127.0.0.1:7700")).isEqualTo(new Args.HostPort("127.0.0.1", 7700));
    assertThat(Args.hostPort("[::1]:80")).isEqualTo(new Args.HostPort("::1", 80));
    assertThatThrownBy(() -> Args.duration("soon")).isInstanceOf(Args.UsageException.class);
    assertThatThrownBy(() -> Args.hostPort("nohost")).isInstanceOf(Args.UsageException.class);
    assertThatThrownBy(() -> Args.hostPort("h:99999")).isInstanceOf(Args.UsageException.class);
  }

  @Test
  void badCommandLinesExitWithUsage() {
    assertThat(run().status).isEqualTo(Main.USAGE);
    assertThat(run("--help").status).isZero();
    assertThat(run("frobnicate").status).isEqualTo(Main.USAGE);
    assertThat(run("kv").status).isEqualTo(Main.USAGE);
    assertThat(run("kv", "put", "only-a-key").status).isEqualTo(Main.USAGE);
    assertThat(run("kv", "get", "a\\q").status).isEqualTo(Main.USAGE);
    assertThat(run("kv", "--bogus", "1", "get", "k").status).isEqualTo(Main.USAGE);
    assertThat(run("kv", "--timeout", "10m", "get", "k").status).isEqualTo(Main.USAGE);
    assertThat(run("node").err).contains("--data-dir is required");
  }

  @Test
  void aWriteToANodeThatIsNotRunningCertainlyFailed() throws Exception {
    int port;
    try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      port = s.getLocalPort();
    }
    Ran put = run("kv", "--server", "127.0.0.1:" + port, "--timeout", "500ms", "put", "k", "v");
    // The connection was refused every time, so nothing was sent: exit 3, not 4.
    assertThat(put.status).as(put.err).isEqualTo(KvCommand.FAILED);
    assertThat(put.err).contains("deadline exceeded").doesNotContain("may or may not");
  }

  private record Ran(int status, String out, String err) {}

  private static Ran run(String... args) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int status =
        Main.run(
            List.of(args),
            new PrintStream(out, true, StandardCharsets.UTF_8),
            new PrintStream(err, true, StandardCharsets.UTF_8));
    return new Ran(
        status, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
  }
}
