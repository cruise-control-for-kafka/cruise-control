/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.linkedin.kafka.cruisecontrol.model.LinearRegressionModelParameters;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner.LoadMonitorTaskRunnerState;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;


@JsonResponseClass
public final class LoadMonitorState {
    @JsonResponseField
    private static final String STATE = "state";
    @JsonResponseField(required = false)
    private static final String REASON_OF_LATEST_PAUSE_OR_RESUME = "reasonOfLatestPauseOrResume";
    @JsonResponseField
    private static final String TRAINED = "trained";
    @JsonResponseField
    private static final String TRAINING_PCT = "trainingPct";
    @JsonResponseField
    private static final String NUM_MONITORED_WINDOWS = "numMonitoredWindows";
    @JsonResponseField(required = false)
    private static final String MONITORED_WINDOWS = "monitoredWindows";
    @JsonResponseField
    private static final String NUM_VALID_PARTITIONS = "numValidPartitions";
    @JsonResponseField
    private static final String NUM_TOTAL_PARTITIONS = "numTotalPartitions";
    @JsonResponseField
    private static final String MONITORING_COVERAGE_PCT = "monitoringCoveragePct";
    @JsonResponseField
    private static final String NUM_FLAWED_PARTITIONS = "numFlawedPartitions";
    @JsonResponseField(required = false)
    private static final String BOOTSTRAP_PROGRESS_PCT = "bootstrapProgressPct";
    @JsonResponseField(required = false)
    private static final String LOADING_PROGRESS_PCT = "loadingProgressPct";
    @JsonResponseField(required = false)
    private static final String ERROR = "error";
    private static final double UNDEFINED_PROGRESS = -1.0;
    private final LoadMonitorTaskRunnerState _loadMonitorTaskRunnerState;
    private final int _numValidWindows;
    private final SortedMap<Long, Float> _monitoredWindows;
    private final double _validPartitionsRatio;
    private final int _numFlawedPartitions;
    private final int _totalNumPartitions;
    private final double _bootstrapProgress;
    private final double _loadingProgress;
    private final String _reasonOfLatestPauseOrResume;

    private LoadMonitorState(LoadMonitorTaskRunnerState state,
                             int numValidWindows,
                             SortedMap<Long, Float> monitoredWindows,
                             double validPartitionsRatio,
                             int totalNumPartitions,
                             int numFlawedPartitions,
                             double bootstrapProgress,
                             double loadingProgress,
                             String reasonOfLatestPauseOrResume) {
        _loadMonitorTaskRunnerState = state;
        _numValidWindows = numValidWindows;
        _monitoredWindows = monitoredWindows;
        _validPartitionsRatio = validPartitionsRatio;
        _numFlawedPartitions = numFlawedPartitions;
        _totalNumPartitions = totalNumPartitions;
        _bootstrapProgress = bootstrapProgress;
        _loadingProgress = loadingProgress;
        _reasonOfLatestPauseOrResume = reasonOfLatestPauseOrResume;
    }

    private LoadMonitorState(LoadMonitorTaskRunnerState state,
                             int numValidWindows,
                             SortedMap<Long, Float> monitoredWindows,
                             double validPartitionsRatio,
                             int totalNumPartitions,
                             int numFlawedPartitions,
                             double bootstrapProgress,
                             double loadingProgress) {
        this(state, numValidWindows, monitoredWindows, validPartitionsRatio, totalNumPartitions, numFlawedPartitions,
                bootstrapProgress, loadingProgress, null);
    }

    public static LoadMonitorState notStarted() {
        return new LoadMonitorState(LoadMonitorTaskRunnerState.NOT_STARTED, 0, null, 0, -1, -1, UNDEFINED_PROGRESS, UNDEFINED_PROGRESS);
    }

    /**
     * Default value for {@link #_bootstrapProgress} and {@link #_loadingProgress} are {@link #UNDEFINED_PROGRESS}.
     *
     * @param numValidWindows                 Number of valid windows.
     * @param partitionCoverageByWindows      Partition coverage by window.
     * @param validPartitionsRatio            Valid partitions ratio.
     * @param totalNumPartitions              Total number of partitions.
     * @param numPartitionsWithExtrapolations Number of partitions with extrapolations.
     * @param reasonOfLatestResume            Reason of the latest resume.
     * @return Running state for load monitor.
     */
    public static LoadMonitorState running(int numValidWindows,
                                           SortedMap<Long, Float> partitionCoverageByWindows,
                                           double validPartitionsRatio,
                                           int totalNumPartitions,
                                           int numPartitionsWithExtrapolations,
                                           String reasonOfLatestResume) {
        return new LoadMonitorState(LoadMonitorTaskRunnerState.RUNNING,
                numValidWindows,
                partitionCoverageByWindows,
                validPartitionsRatio,
                totalNumPartitions,
                numPartitionsWithExtrapolations,
                UNDEFINED_PROGRESS,
                UNDEFINED_PROGRESS,
                reasonOfLatestResume);
    }

    /**
     * Default value for {@link #_bootstrapProgress} and {@link #_loadingProgress} are {@link #UNDEFINED_PROGRESS}.
     *
     * @param numValidWindows                 Number of valid windows.
     * @param monitoredSnapshotWindows        Monitored snapshot windows.
     * @param validPartitionsRatio            Valid partitions ratio.
     * @param totalNumPartitions              Total number of partitions.
     * @param numPartitionsWithExtrapolations Number of partitions with extrapolations.
     * @param reasonOfLatestPause             Reason of the latest pause.
     * @return Paused state for load monitor.
     */
    public static LoadMonitorState paused(int numValidWindows,
                                          SortedMap<Long, Float> monitoredSnapshotWindows,
                                          double validPartitionsRatio,
                                          int totalNumPartitions,
                                          int numPartitionsWithExtrapolations,
                                          String reasonOfLatestPause) {
        return new LoadMonitorState(LoadMonitorTaskRunnerState.PAUSED,
                numValidWindows,
                monitoredSnapshotWindows,
                validPartitionsRatio,
                totalNumPartitions,
                numPartitionsWithExtrapolations,
                UNDEFINED_PROGRESS,
                UNDEFINED_PROGRESS,
                reasonOfLatestPause);
    }

    /**
     * Default value for {@link #_bootstrapProgress} and {@link #_loadingProgress} are {@link #UNDEFINED_PROGRESS}.
     *
     * @param numValidWindows                 Number of valid windows.
     * @param monitoredSnapshotWindows        Monitored snapshot windows.
     * @param validPartitionsRatio            Valid partitions ratio.
     * @param totalNumPartitions              Total number of partitions.
     * @param numPartitionsWithExtrapolations Number of partitions with extrapolations.
     * @return Sampling state for load monitor.
     */
    public static LoadMonitorState sampling(int numValidWindows,
                                            SortedMap<Long, Float> monitoredSnapshotWindows,
                                            double validPartitionsRatio,
                                            int totalNumPartitions,
                                            int numPartitionsWithExtrapolations) {
        return new LoadMonitorState(LoadMonitorTaskRunnerState.SAMPLING,
                numValidWindows,
                monitoredSnapshotWindows,
                validPartitionsRatio,
                totalNumPartitions,
                numPartitionsWithExtrapolations,
                UNDEFINED_PROGRESS,
                UNDEFINED_PROGRESS);
    }

    /**
     * Default value for {@link #_bootstrapProgress} and {@link #_loadingProgress} are {@link #UNDEFINED_PROGRESS}.
     *
     * @param numValidWindows                 Number of valid windows.
     * @param monitoredSnapshotWindows        Monitored snapshot windows.
     * @param validPartitionsRatio            Valid partitions ratio.
     * @param totalNumPartitions              Total number of partitions.
     * @param bootstrapProgress               Bootstrap progress -- should be between 0 and 1.
     * @param numPartitionsWithExtrapolations Number of partitions with extrapolations.
     * @return Bootstrapping state for load monitor.
     */
    public static LoadMonitorState bootstrapping(int numValidWindows,
                                                 SortedMap<Long, Float> monitoredSnapshotWindows,
                                                 double validPartitionsRatio,
                                                 int totalNumPartitions,
                                                 double bootstrapProgress,
                                                 int numPartitionsWithExtrapolations) {
        return new LoadMonitorState(LoadMonitorTaskRunnerState.BOOTSTRAPPING,
                numValidWindows,
                monitoredSnapshotWindows,
                validPartitionsRatio,
                totalNumPartitions,
                numPartitionsWithExtrapolations,
                bootstrapProgress,
                UNDEFINED_PROGRESS);
    }

    /**
     * Default value for {@link #_bootstrapProgress} and {@link #_loadingProgress} are {@link #UNDEFINED_PROGRESS}.
     *
     * @param numValidWindows                 Number of valid windows.
     * @param monitoredSnapshotWindows        Monitored snapshot windows.
     * @param validPartitionsRatio            Valid partitions ratio.
     * @param totalNumPartitions              Total number of partitions.
     * @param numPartitionsWithExtrapolations Number of partitions with extrapolations.
     * @return Training state for load monitor.
     */
    public static LoadMonitorState training(int numValidWindows,
                                            SortedMap<Long, Float> monitoredSnapshotWindows,
                                            double validPartitionsRatio,
                                            int totalNumPartitions,
                                            int numPartitionsWithExtrapolations) {
        return new LoadMonitorState(LoadMonitorTaskRunnerState.TRAINING,
                numValidWindows,
                monitoredSnapshotWindows,
                validPartitionsRatio,
                totalNumPartitions,
                numPartitionsWithExtrapolations,
                UNDEFINED_PROGRESS,
                UNDEFINED_PROGRESS);
    }

    /**
     * Default value for {@link #_bootstrapProgress} and {@link #_loadingProgress} are {@link #UNDEFINED_PROGRESS}.
     *
     * @param numValidWindows          Number of valid windows.
     * @param monitoredSnapshotWindows Monitored snapshot windows.
     * @param validPartitionsRatio     Valid partitions ratio.
     * @param totalNumPartitions       Total number of partitions.
     * @param loadingProgress          Loading progress -- should be between 0 and 1.
     * @return Loading state for load monitor.
     */
    public static LoadMonitorState loading(int numValidWindows,
                                           SortedMap<Long, Float> monitoredSnapshotWindows,
                                           double validPartitionsRatio,
                                           int totalNumPartitions,
                                           double loadingProgress) {
        return new LoadMonitorState(LoadMonitorTaskRunnerState.LOADING,
                numValidWindows,
                monitoredSnapshotWindows,
                validPartitionsRatio,
                totalNumPartitions,
                -1,
                UNDEFINED_PROGRESS,
                loadingProgress);
    }

    /**
     * Set the common JSON generic attribute collection.
     *
     * @param verbose          {@code true} if verbose, {@code false} otherwise.
     * @param loadMonitorState Load monitor state.
     */
    private void setCommonJsonGenericAttributeCollection(boolean verbose, Map<String, Object> loadMonitorState) {
        loadMonitorState.put(STATE, _loadMonitorTaskRunnerState);
        loadMonitorState.put(TRAINED, ModelParameters.trainingCompleted());
        loadMonitorState.put(TRAINING_PCT, ModelParameters.trainingCompleted()
                ? 100.0 : (ModelParameters.modelCoefficientTrainingCompleteness() * 100));
        loadMonitorState.put(NUM_MONITORED_WINDOWS, _monitoredWindows.size());
        if (verbose) {
            loadMonitorState.put(MONITORED_WINDOWS, _monitoredWindows);
        }
        loadMonitorState.put(NUM_VALID_PARTITIONS, (int) (_validPartitionsRatio * _totalNumPartitions));
        loadMonitorState.put(NUM_TOTAL_PARTITIONS, _totalNumPartitions);
        loadMonitorState.put(MONITORING_COVERAGE_PCT, nanToZero(validPartitionsRatio() * 100));
        loadMonitorState.put(NUM_FLAWED_PARTITIONS, _numFlawedPartitions);
    }

    /**
     * @param verbose {@code true} if verbose, {@code false} otherwise.
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure(boolean verbose) {
        Map<String, Object> loadMonitorState = new HashMap<>();
        // generic attribute collection
        switch (_loadMonitorTaskRunnerState) {
            case RUNNING:
            case SAMPLING:
            case PAUSED:
            case BOOTSTRAPPING:
            case TRAINING:
            case LOADING:
                setCommonJsonGenericAttributeCollection(verbose, loadMonitorState);
                break;
            default:
                break;
        }
        // specific attribute collection
        switch (_loadMonitorTaskRunnerState) {
            case RUNNING:
            case PAUSED:
                loadMonitorState.put(REASON_OF_LATEST_PAUSE_OR_RESUME, _reasonOfLatestPauseOrResume == null ? "N/A" : _reasonOfLatestPauseOrResume);
                break;
            case SAMPLING:
            case TRAINING:
                break;
            case BOOTSTRAPPING:
                loadMonitorState.put(BOOTSTRAP_PROGRESS_PCT, nanToZero(_bootstrapProgress * 100));
                break;
            case LOADING:
                loadMonitorState.put(LOADING_PROGRESS_PCT, nanToZero(_loadingProgress * 100));
                break;
            default:
                loadMonitorState.put(STATE, _loadMonitorTaskRunnerState);
                loadMonitorState.put(ERROR, "ILLEGAL_STATE_EXCEPTION");
                break;
        }
        return loadMonitorState;
    }

    /*
     * JSON does not support literal NaN value
     * round it to zero when Java Math sees a NaN
     */
    public static double nanToZero(double v) {
        return Double.isNaN(v) ? 0.0 : v;
    }

    @Override
    public String toString() {
        String trained = ModelParameters.trainingCompleted()
                ? "(TRAINED)"
                : String.format("(%.3f%% trained)", ModelParameters.modelCoefficientTrainingCompleteness() * 100);
        float validWindowPercent = (float) _numValidWindows / _monitoredWindows.size();
        switch (_loadMonitorTaskRunnerState) {
            case NOT_STARTED:
                return String.format("{state: %s}", _loadMonitorTaskRunnerState);

            case RUNNING:
            case PAUSED:
                return String.format("{state: %s%s, NumValidWindows: (%d/%d) (%.3f%%), NumValidPartitions: %d/%d (%.3f%%), flawedPartitions: %d%s}",
                        _loadMonitorTaskRunnerState, trained, _numValidWindows,
                        _monitoredWindows.size(), validWindowPercent * 100,
                        numValidPartitions(), _totalNumPartitions, _validPartitionsRatio * 100,
                        _numFlawedPartitions, _reasonOfLatestPauseOrResume == null
                                ? "" : String.format(", reasonOfPauseOrResume: %s", _reasonOfLatestPauseOrResume));
            case SAMPLING:
                return String.format("{state: %s%s, NumValidWindows: (%d/%d) (%.3f%%), NumValidPartitions: %d/%d (%.3f%%), flawedPartitions: %d}",
                        _loadMonitorTaskRunnerState, trained, _numValidWindows,
                        _monitoredWindows.size(), validWindowPercent * 100,
                        numValidPartitions(), _totalNumPartitions, _validPartitionsRatio * 100,
                        _numFlawedPartitions);
            case BOOTSTRAPPING:
                return String.format("{state: %s%s, BootstrapProgress: %.3f%%, NumValidWindows: (%d/%d) (%.3f%%), "
                                + "NumValidPartitions: %d/%d (%.3f%%), flawedPartitions: %d}",
                        _loadMonitorTaskRunnerState, trained, _bootstrapProgress * 100, _numValidWindows, _monitoredWindows
                                .size(), validWindowPercent * 100,
                        numValidPartitions(), _totalNumPartitions, _validPartitionsRatio * 100,
                        _numFlawedPartitions);

            case TRAINING:
                return String.format("{state: %s%s, NumValidWindows: (%d/%d) (%.3f%%) , NumValidPartitions: %d/%d (%.3f%%), FlawedPartitions: %d}",
                        _loadMonitorTaskRunnerState, trained, _monitoredWindows.size(), _numValidWindows,
                        validWindowPercent * 100, numValidPartitions(), _totalNumPartitions, _validPartitionsRatio * 100,
                        _numFlawedPartitions);

            case LOADING:
                return String.format("{state: %s%s, LoadingProgress: %.3f%%, NumValidWindows: (%d/%d): (%.3f%%), "
                                + "NumValidPartitions: %d/%d (%.3f%%), FlawedPartitions: %d}",
                        _loadMonitorTaskRunnerState, trained, _loadingProgress * 100, _numValidWindows,
                        _monitoredWindows.size(), validWindowPercent * 100,
                        numValidPartitions(), _totalNumPartitions, _validPartitionsRatio * 100,
                        _numFlawedPartitions);
            default:
                throw new IllegalStateException("Should never be here");
        }
    }

    public LoadMonitorTaskRunnerState state() {
        return _loadMonitorTaskRunnerState;
    }

    public int numValidWindows() {
        return _numValidWindows;
    }

    public SortedMap<Long, Float> monitoredWindows() {
        return _monitoredWindows;
    }

    public int numValidPartitions() {
        return (int) (_validPartitionsRatio * _totalNumPartitions);
    }

    public double validPartitionsRatio() {
        return _validPartitionsRatio;
    }

    public double bootstrapProgress() {
        return _bootstrapProgress;
    }

    @Nonnull
    public LinearRegressionModelParameters.LinearRegressionModelState detailTrainingProgress() {
        return ModelParameters.linearRegressionModelState();
    }
}
