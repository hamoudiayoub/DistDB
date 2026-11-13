package io.distdb.sim;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;

import com.tngtech.archunit.lang.ArchRule;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.Timer;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;

/** The architecture rules, kept separate so a test can check they really catch violations. */
final class ArchitectureRules {

  /** Modules whose logic must be deterministic, i.e. reach the world only through distdb-env. */
  static final String[] CORE_PACKAGES = {
    "io.distdb.storage..",
    "io.distdb.raft..",
    "io.distdb.kv..",
    "io.distdb.pd..",
    "io.distdb.client..",
    "io.distdb.txn..",
    "io.distdb.sql..",
  };

  /** Dependencies point only downward, and only to the layers each module is meant to use. */
  static final ArchRule LAYERS =
      layeredArchitecture()
          .consideringOnlyDependenciesInLayers()
          .withOptionalLayers(true)
          .layer("common")
          .definedBy("io.distdb.common..")
          .layer("env")
          .definedBy("io.distdb.env..")
          .layer("storage")
          .definedBy("io.distdb.storage..")
          .layer("raft")
          .definedBy("io.distdb.raft..")
          .layer("rpc")
          .definedBy("io.distdb.rpc..")
          .layer("kv")
          .definedBy("io.distdb.kv..")
          .layer("pd")
          .definedBy("io.distdb.pd..")
          .layer("client")
          .definedBy("io.distdb.client..")
          .layer("txn")
          .definedBy("io.distdb.txn..")
          .layer("sql")
          .definedBy("io.distdb.sql..")
          .layer("pgwire")
          .definedBy("io.distdb.pgwire..")
          .layer("server")
          .definedBy("io.distdb.server..")
          .layer("sim")
          .definedBy("io.distdb.sim..")
          .layer("bench")
          .definedBy("io.distdb.bench..")
          .whereLayer("common")
          .mayNotAccessAnyLayer()
          .whereLayer("env")
          .mayOnlyAccessLayers("common")
          .whereLayer("storage")
          .mayOnlyAccessLayers("env", "common")
          .whereLayer("raft")
          .mayOnlyAccessLayers("env", "common")
          .whereLayer("rpc")
          .mayOnlyAccessLayers("raft", "env", "common")
          .whereLayer("kv")
          .mayOnlyAccessLayers("storage", "raft", "rpc", "env", "common")
          .whereLayer("pd")
          .mayOnlyAccessLayers("kv", "client", "storage", "raft", "rpc", "env", "common")
          .whereLayer("client")
          .mayOnlyAccessLayers("rpc", "env", "common")
          .whereLayer("txn")
          .mayOnlyAccessLayers("client", "rpc", "env", "common")
          .whereLayer("sql")
          .mayOnlyAccessLayers("txn", "client", "rpc", "env", "common")
          .whereLayer("pgwire")
          .mayOnlyAccessLayers("sql", "env", "common")
          .whereLayer("sim")
          .mayNotBeAccessedByAnyLayer()
          .whereLayer("bench")
          .mayNotBeAccessedByAnyLayer();

  /**
   * Core logic never reads the clock, draws randomness, sleeps, starts threads, or touches the
   * network or disk directly; it goes through Clock, RandomSource, Scheduler, Transport and
   * FileSystem, so the simulator can replace all of them and replay any run from its seed.
   */
  static final ArchRule CORE_IS_DETERMINISTIC =
      noClasses()
          .that()
          .resideInAnyPackage(CORE_PACKAGES)
          .should()
          .callMethod(System.class, "currentTimeMillis")
          .orShould()
          .callMethod(System.class, "nanoTime")
          .orShould()
          .callMethod(Thread.class, "sleep", long.class)
          .orShould()
          .callMethod(Thread.class, "sleep", Duration.class)
          .orShould()
          .callMethod(Thread.class, "ofVirtual")
          .orShould()
          .callMethod(Thread.class, "ofPlatform")
          .orShould()
          .callMethod(Thread.class, "startVirtualThread", Runnable.class)
          .orShould()
          .callConstructorWhere(
              com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                  com.tngtech.archunit.core.domain.properties.HasOwner.Predicates.With.owner(
                      com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo(
                          Thread.class))))
          .orShould()
          .callMethod(Instant.class, "now")
          .orShould()
          .callMethod(LocalDate.class, "now")
          .orShould()
          .callMethod(LocalDateTime.class, "now")
          .orShould()
          .callMethod(OffsetDateTime.class, "now")
          .orShould()
          .callMethod(ZonedDateTime.class, "now")
          .orShould()
          .dependOnClassesThat()
          .belongToAnyOf(
              java.time.Clock.class,
              Random.class,
              SecureRandom.class,
              SplittableRandom.class,
              ThreadLocalRandom.class,
              Executors.class,
              ForkJoinPool.class,
              Timer.class,
              Socket.class,
              ServerSocket.class,
              DatagramSocket.class,
              SocketChannel.class,
              ServerSocketChannel.class,
              FileChannel.class,
              AsynchronousFileChannel.class,
              Files.class,
              RandomAccessFile.class,
              FileInputStream.class,
              FileOutputStream.class)
          .allowEmptyShould(true)
          .because(
              "core logic must reach time, randomness, threads, network and disk only through"
                  + " the distdb-env seams, so every simulation run is reproducible from its seed");

  private ArchitectureRules() {}
}
