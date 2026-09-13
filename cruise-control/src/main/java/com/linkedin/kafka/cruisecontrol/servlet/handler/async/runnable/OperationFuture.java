/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


/**
 * The future to support async operations in KafkaCruiseControl
 */
public class OperationFuture extends CompletableFuture<CruiseControlResponse> {
  // The url encoded request url
  protected final String _operation;
  protected final OperationProgress _operationProgress;
  protected volatile Thread _executionThread = null;
  protected long _finishTimeNs;

  public OperationFuture(String operation) {
    _operation = operation;
    _operationProgress = new OperationProgress(operation);
    _finishTimeNs = -1;
  }

  @Override
  public synchronized boolean cancel(boolean mayInterruptIfRunning) {
    boolean canceled = false;
    if (_executionThread != null) {
      if (mayInterruptIfRunning) {
        canceled = super.cancel(true);
        _executionThread.interrupt();
      }
    } else {
      canceled = super.cancel(true);
    }
    notifyAll();
    return canceled;
  }

  @Override
  public CruiseControlResponse get() throws InterruptedException, ExecutionException {
    // Annotate failures with the operation context by rethrowing a new exception instance. Mutating the
    // original exception's detailMessage via reflection is not possible on Java 9+ without --add-opens
    // and threw InaccessibleObjectException at runtime on Java 17.
    try {
      return super.get();
    } catch (ExecutionException ee) {
      ExecutionException annotated = new ExecutionException(annotatedMessage(ee), ee.getCause());
      annotated.setStackTrace(ee.getStackTrace());
      throw annotated;
    } catch (CancellationException ce) {
      CancellationException annotated = new CancellationException(annotatedMessage(ce));
      annotated.initCause(ce);
      annotated.setStackTrace(ce.getStackTrace());
      throw annotated;
    } catch (InterruptedException ie) {
      InterruptedException annotated = new InterruptedException(annotatedMessage(ie));
      annotated.setStackTrace(ie.getStackTrace());
      throw annotated;
    }
  }

  private String annotatedMessage(Throwable t) {
    return String.format("Operation '%s' received exception. ", _operation) + (t.getMessage() == null ? "" : t.getMessage());
  }

  /**
   * @return The operation for this future.
   */
  public String operation() {
    return _operation;
  }

  /**
   * Set the execution thread to allow cancellation. The set will be successful unless the future is canceled
   * and a non-null thread is set to be execution thread.
   *
   * @param t the thread working for this future.
   * @return {@code true} if the execution thread is set successfully, {@code false} otherwise.
   */
  public synchronized boolean setExecutionThread(Thread t) {
    // The setting only fails if a thread tries to pick up a canceled operation.
    if (isCancelled() && t != null) {
      return false;
    } else {
      _executionThread = t;
      return true;
    }
  }

  /**
   * @return The string describing the progress of the operation.
   */
  public String progressString() {
    return _operationProgress.toString();
  }

  /**
   * @return The map describing the progress of the operation.
   */
  public Map<String, Object> getJsonStructure() {
    return _operationProgress.getJsonStructure();
  }

  /**
   * @return The {@link OperationProgress} of this operation.
   */
  public OperationProgress operationProgress() {
    return _operationProgress;
  }

  /**
   * Record the finish time of this operation, invoked at the end of corresponding {@link OperationRunnable#run()}.
   * @param finishTimeNs the system time when this operation completes.
   */
  public void setFinishTimeNs(long finishTimeNs) {
    _finishTimeNs = finishTimeNs;
  }

  /**
   * @return The integer representing the finish time of this operation.
   */
  public long finishTimeNs() {
    return _finishTimeNs;
  }
}
