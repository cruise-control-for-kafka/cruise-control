/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The default {@link ExecutionStateStore}, which keeps the execution state as a JSON document in a local file (see
 * {@link ExecutorConfig#EXECUTOR_STATE_FILE_PATH_CONFIG}).
 *
 * <ul>
 *   <li>Each save writes to a temporary file and atomically moves it over the state file, so a crash in the middle of a
 *   save never leaves a partially written state behind. The temporary file is synced to the storage device before the move,
 *   and the directory after it (where supported), so that a power loss cannot leave an empty or stale state file either.</li>
 *   <li>The state file is readable and writable only by its owner (if supported by the file system).</li>
 *   <li>A state file that cannot be parsed is renamed with a {@value #CORRUPT_FILE_SUFFIX} suffix (for later inspection)
 *   and treated as if there is no stored state -- so that it does not block every subsequent restart.</li>
 * </ul>
 */
public class FileExecutionStateStore implements ExecutionStateStore {
  private static final Logger LOG = LoggerFactory.getLogger(FileExecutionStateStore.class);
  public static final String TEMP_FILE_SUFFIX = ".tmp";
  public static final String CORRUPT_FILE_SUFFIX = ".corrupt";
  private Path _path;
  private Path _tempPath;

  @Override
  public synchronized void configure(Map<String, ?> configs) {
    Object path = configs.get(ExecutorConfig.EXECUTOR_STATE_FILE_PATH_CONFIG);
    String pathString = path == null ? ExecutorConfig.DEFAULT_EXECUTOR_STATE_FILE_PATH : path.toString();
    if (pathString.isBlank()) {
      throw new IllegalArgumentException(ExecutorConfig.EXECUTOR_STATE_FILE_PATH_CONFIG + " cannot be empty.");
    }
    _path = Paths.get(pathString);
    _tempPath = Paths.get(pathString + TEMP_FILE_SUFFIX);
  }

  /**
   * @return Path of the state file.
   */
  public synchronized Path path() {
    return _path;
  }

  @Override
  public synchronized void save(PersistedExecutionState state) throws IOException {
    ensureConfigured();
    Path parent = _path.toAbsolutePath().getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    try (FileChannel channel = FileChannel.open(_tempPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                                                StandardOpenOption.WRITE);
         Writer writer = new BufferedWriter(Channels.newWriter(channel, StandardCharsets.UTF_8))) {
      PersistedExecutionStateSerde.writeJson(state, writer);
      writer.flush();
      channel.force(true);
    }
    try {
      Files.setPosixFilePermissions(_tempPath, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    } catch (UnsupportedOperationException | IOException e) {
      LOG.debug("Unable to restrict the file permissions of the execution state file {}.", _tempPath, e);
    }
    try {
      Files.move(_tempPath, _path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(_tempPath, _path, StandardCopyOption.REPLACE_EXISTING);
    }
    syncDirectory(parent);
  }

  @Override
  public synchronized Optional<PersistedExecutionState> load() throws IOException {
    ensureConfigured();
    String json;
    try {
      json = Files.readString(_path, StandardCharsets.UTF_8);
    } catch (NoSuchFileException e) {
      return Optional.empty();
    }
    try {
      return Optional.of(PersistedExecutionStateSerde.fromJson(json));
    } catch (IllegalArgumentException e) {
      Path corruptPath = Paths.get(_path + CORRUPT_FILE_SUFFIX);
      LOG.error("Failed to parse the execution state file {}. Moving it to {} and ignoring it.", _path, corruptPath, e);
      Files.move(_path, corruptPath, StandardCopyOption.REPLACE_EXISTING);
      return Optional.empty();
    }
  }

  @Override
  public synchronized void delete() throws IOException {
    ensureConfigured();
    Files.deleteIfExists(_path);
    Files.deleteIfExists(_tempPath);
    syncDirectory(_path.toAbsolutePath().getParent());
  }

  /**
   * Sync the given directory, so that a rename or deletion in it survives a power loss. Not all platforms support it -- e.g.
   * opening a directory fails on Windows -- in which case this is a no-op.
   *
   * @param directory Directory to sync, or {@code null} to do nothing.
   */
  private static void syncDirectory(Path directory) {
    if (directory == null) {
      return;
    }
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    } catch (IOException | UnsupportedOperationException e) {
      LOG.debug("Unable to sync the directory {} of the execution state file.", directory, e);
    }
  }

  private void ensureConfigured() {
    if (_path == null) {
      throw new IllegalStateException("The execution state store has not been configured.");
    }
  }
}
