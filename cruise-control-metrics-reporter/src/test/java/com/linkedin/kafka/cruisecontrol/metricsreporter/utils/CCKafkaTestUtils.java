/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import java.util.stream.Stream;


public final class CCKafkaTestUtils {
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
}
