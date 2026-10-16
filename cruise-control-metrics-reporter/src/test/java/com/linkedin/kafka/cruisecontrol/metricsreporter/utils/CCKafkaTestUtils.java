/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;


public final class CCKafkaTestUtils {
  private static final AtomicBoolean SHUTDOWN_HOOK_INSTALLED = new AtomicBoolean(false);
  private static final Thread SHUTDOWN_HOOK;
  private static final List<File> FILES_TO_CLEAN_UP = Collections.synchronizedList(new ArrayList<>());

  static {
    SHUTDOWN_HOOK = new Thread(() -> {
      Exception firstIssue = null;
      for (File toCleanUp : FILES_TO_CLEAN_UP) {
        if (!toCleanUp.exists()) {
          continue;
        }
        try {
          deleteRecursively(toCleanUp);
        } catch (IOException issue) {
          if (firstIssue == null) {
            firstIssue = issue;
          } else {
            firstIssue.addSuppressed(issue);
          }
        }
      }
      if (firstIssue != null) {
        System.err.println("unable to delete one or more files");
        firstIssue.printStackTrace(System.err);
        throw new IllegalStateException(firstIssue);
      }
    }, "CCKafkaTestUtils cleanup hook");
    SHUTDOWN_HOOK.setUncaughtExceptionHandler((t, e) -> {
      System.err.println("thread " + t.getName() + " died to uncaught exception");
      e.printStackTrace(System.err);
    });
  }

  private CCKafkaTestUtils() {
    //utility class
  }

  /**
   * Get a producer connected to the given broker.
   *
   * @param broker The broker to connect to.
   * @return A producer connected to the given broker.
   */
  public static KafkaProducer<String, String> producerFor(CCEmbeddedBroker broker) {
    String bootstrap = broker.plaintextAddr();
    if (bootstrap == null) {
      bootstrap = broker.sslAddr();
    }

    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024 * 1024);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    return new KafkaProducer<>(props);
  }

  /**
   * Get a consumer connected to the given broker.
   *
   * @param broker The broker to connect to.
   * @return A consumer connected to the given broker.
   */
  public static KafkaConsumer<String, String> consumerFor(CCEmbeddedBroker broker) {
    String bootstrap = broker.plaintextAddr();
    if (bootstrap == null) {
      bootstrap = broker.sslAddr();
    }

    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    return new KafkaConsumer<>(props);
  }

  /**
   * @return A new temp directory.
   */
  public static File newTempDir() {
    try {
      return cleanup(Files.createTempDirectory(null).toFile());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Cleanup the given file.
   *
   * @param toCleanUp File to cleanup.
   * @return File to be cleaned up.
   */
  public static File cleanup(File toCleanUp) {
    if (SHUTDOWN_HOOK_INSTALLED.compareAndSet(false, true)) {
      Runtime.getRuntime().addShutdownHook(SHUTDOWN_HOOK);
    }
    FILES_TO_CLEAN_UP.add(toCleanUp);
    return toCleanUp;
  }

  /**
   * Run the given task and catch exception if the task throws one.
   *
   * @param task Task to run.
   */
  public static void quietly(Task task) {
    try {
      task.run();
    } catch (Exception e) {
      e.printStackTrace(System.err);
    }
  }

  /**
   * Get a random string with the given length.
   *
   * @param length The length of the random String.
   * @return Random String.
   */
  public static String getRandomString(int length) {
    char[] chars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    Random random = new Random();
    StringBuilder stringBuiler = new StringBuilder();
    for (int i = 0; i < length; i++) {
      stringBuiler.append(chars[Math.abs(random.nextInt()) % 16]);
    }
    return stringBuiler.toString();
  }

  /**
   * Find a local port.
   * @return A local port to use.
   */
  public static int findLocalPort() {
    int port = -1;
    while (port < 0) {
      try {
        ServerSocket socket = new ServerSocket(0);
        socket.setReuseAddress(true);
        port = socket.getLocalPort();
        try {
          socket.close();
        } catch (IOException e) {
          // Ignore IOException on close()
        }
      } catch (IOException ie) {
        // let it go.
      }
    }
    return port;
  }

  /**
   * Recursively delete a file or directory.
   * @param file the file or directory to delete
   */
  public static void deleteRecursively(File file) throws IOException {
    if (!file.exists()) {
      return;
    }
    try (Stream<Path> paths = Files.walk(file.toPath())) {
      paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }

  /**
   * A functional interface for a task to run.
   */
  @FunctionalInterface
  public interface Task {
    void run() throws Exception;
  }
}
