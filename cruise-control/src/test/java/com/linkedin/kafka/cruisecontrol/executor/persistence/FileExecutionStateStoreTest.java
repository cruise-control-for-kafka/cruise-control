/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.persistence.PersistedExecutionStateSerdeTest.assertStateEquals;
import static com.linkedin.kafka.cruisecontrol.executor.persistence.PersistedExecutionStateSerdeTest.sampleState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class FileExecutionStateStoreTest {
  private static final String NESTED_DIRECTORY = "nested";
  private Path _directory;
  private Path _statePath;
  private FileExecutionStateStore _store;

  /**
   * Setup the test.
   */
  @Before
  public void setUp() throws IOException {
    _directory = Files.createTempDirectory("execution-state-store-test");
    _statePath = _directory.resolve(NESTED_DIRECTORY).resolve("executionState.json");
    _store = new FileExecutionStateStore();
    _store.configure(Collections.singletonMap(ExecutorConfig.EXECUTOR_STATE_FILE_PATH_CONFIG, _statePath.toString()));
  }

  /**
   * Teardown the test.
   */
  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(_directory.toFile());
  }

  @Test
  public void testLoadWithoutStoredState() throws IOException {
    assertEquals(Optional.empty(), _store.load());
  }

  @Test
  public void testSaveLoadAndDelete() throws IOException {
    PersistedExecutionState state = sampleState();
    _store.save(state);
    assertTrue(Files.exists(_statePath));
    assertFalse(Files.exists(Paths.get(_statePath + FileExecutionStateStore.TEMP_FILE_SUFFIX)));
    assertStateEquals(state, _store.load().orElseThrow());

    // Overwrite the stored state.
    PersistedExecutionState updatedState = state.toBuilder().lastUpdateTimeMs(state.lastUpdateTimeMs() + 1).tasks(Collections.emptyList())
                                                .build();
    _store.save(updatedState);
    assertStateEquals(updatedState, _store.load().orElseThrow());

    _store.delete();
    assertFalse(Files.exists(_statePath));
    assertEquals(Optional.empty(), _store.load());
    // Deleting a non-existing state is a no-op.
    _store.delete();
  }

  @Test
  public void testStateFileIsOwnerReadableOnly() throws IOException {
    Assume.assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    _store.save(sampleState());
    assertEquals(EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE), Files.getPosixFilePermissions(_statePath));
  }

  @Test
  public void testCorruptStateFileIsSetAside() throws IOException {
    Files.createDirectories(_directory.resolve(NESTED_DIRECTORY));
    Files.writeString(_statePath, "{not a valid execution state", StandardCharsets.UTF_8);
    assertEquals(Optional.empty(), _store.load());
    assertFalse(Files.exists(_statePath));
    assertTrue(Files.exists(Paths.get(_statePath + FileExecutionStateStore.CORRUPT_FILE_SUFFIX)));
  }

  @Test
  public void testConfigure() {
    FileExecutionStateStore store = new FileExecutionStateStore();
    assertThrows(IllegalStateException.class, store::load);
    store.configure(Collections.emptyMap());
    assertEquals(Paths.get(ExecutorConfig.DEFAULT_EXECUTOR_STATE_FILE_PATH), store.path());
    assertThrows(IllegalArgumentException.class,
                 () -> store.configure(Collections.singletonMap(ExecutorConfig.EXECUTOR_STATE_FILE_PATH_CONFIG, " ")));
  }

  @Test
  public void testNoopStore() throws IOException {
    NoopExecutionStateStore store = new NoopExecutionStateStore();
    store.configure(Collections.emptyMap());
    store.save(sampleState());
    assertEquals(Optional.empty(), store.load());
    store.delete();
  }
}
