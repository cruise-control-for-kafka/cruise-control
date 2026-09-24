/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationStep;
import com.linkedin.kafka.cruisecontrol.async.progress.Pending;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * An abstract class for the async operation runnables.
 */
abstract class OperationRunnable implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(OperationRunnable.class);
    protected final KafkaCruiseControl _kafkaCruiseControl;
    protected final OperationFuture _future;

    OperationRunnable(KafkaCruiseControl kafkaCruiseControl, OperationFuture future) {
        _kafkaCruiseControl = kafkaCruiseControl;
        _future = future;
    }

    /**
     * @return The corresponding {@link OperationFuture} for the runnable.
     */
    protected OperationFuture future() {
        return _future;
    }

    @Override
    public void run() {
        try {
            // Only execute the runnable if the future is not canceled.
            if (_future.setExecutionThread(Thread.currentThread())) {
                List<OperationStep> steps = _future.operationProgress().progress();
                if (!steps.isEmpty() && steps.get(0) instanceof Pending) {
                    ((Pending) steps.get(0)).done();
                }
                _future.complete(getResult());
                // If operation completes successfully (i.e with no exception thrown), record the operation finish time.
                _future.setFinishTimeNs(System.nanoTime());
            }
        } catch (Exception e) {
            LOG.warn("Received exception when trying to execute runnable for \"" + _future.operation() + "\"", e);
            _future.completeExceptionally(e);
        } finally {
            _future.setExecutionThread(null);
            // Clear the interrupt flag after removing itself from the execution thread.
            Thread.interrupted();
        }
    }

    protected abstract CruiseControlResponse getResult() throws Exception;
}
