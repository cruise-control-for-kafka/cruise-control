/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.*;


public class LinearRegressionModelParameters {
    private static final Logger LOG = LoggerFactory.getLogger(LinearRegressionModelParameters.class);
    private static final double LEADER_BYTES_IN_AND_OUT_DIVERSITY_THRESHOLD = 0.5;
    private static int minCpuUtilObservationBuckets;
    private static int cpuUtilBucketSize;
    // The metric observations we are going to use to do the linear regression. We just hard code it to 100 observations
    // for each CPU utilization bucket.
    private static int numObservationsPerUtilBucket;
    private static final Map<Integer, double[][]> BYTE_RATE_OBSERVATIONS = new HashMap<>();
    private static final ConcurrentMap<Integer, double[]> CPU_UTIL_OBSERVATIONS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Integer, AtomicInteger> INDICES = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<Integer, Integer> OBSERVED_LEADER_TO_FOLLOWER_BYTES_RATIO = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<Integer, Integer> OBSERVED_LEADER_BYTES_IN_TO_BYTES_OUT_RATIO = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<Integer, Integer> CPU_UTIL_ESTIMATION_ERROR_STATS = new ConcurrentSkipListMap<>();
    private static final Map<ModelCoefficient, Double> COEFFICIENTS = new HashMap<>();

    static void init(KafkaCruiseControlConfig config) {
        minCpuUtilObservationBuckets = config.getInt(MonitorConfig.LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_CONFIG);
        cpuUtilBucketSize = config.getInt(MonitorConfig.LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE_CONFIG);
        numObservationsPerUtilBucket = config.getInt(MonitorConfig.LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET_CONFIG);
        int numBuckets = 99 / cpuUtilBucketSize + 1;
        if (minCpuUtilObservationBuckets > numBuckets) {
            throw new IllegalArgumentException("There are only " + numBuckets + " CPU utilization buckets with "
                    + cpuUtilBucketSize + "%% bucket size. But "
                    + MonitorConfig.LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_CONFIG + " is "
                    + minCpuUtilObservationBuckets
            );
        }
    }

    public synchronized boolean trainingCompleted() {
        return !COEFFICIENTS.isEmpty();
    }

    public Double getCoefficient(ModelCoefficient coefficient) {
        return COEFFICIENTS.get(coefficient);
    }

    /**
     * Trigger the calculation of the model parameters.
     *
     * @return {@code true} if the parameters are generated, otherwise {@code false};
     */
    public synchronized boolean updateModelCoefficient() {
        if (validBuckets().size() < minCpuUtilObservationBuckets) {
            return false;
        }
        try {
            OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
            regression.setNoIntercept(true);
            boolean ignoreLeaderBytesOut = isLeaderBytesInAndOutRatioDiverseEnough();
            regression.newSampleData(aggregateSampleCpuUtilData(),
                    aggregateSampleBytesRateData(ignoreLeaderBytesOut));
            double[] parameters = regression.estimateRegressionParameters();
            int leaderBytesInIndex = 0;
            int leaderBytesOutIndex = 1;
            int followerBytesInIndex = ignoreLeaderBytesOut ? 1 : 2;
            COEFFICIENTS.put(ModelCoefficient.LEADER_BYTES_IN, parameters[leaderBytesInIndex]);
            if (!ignoreLeaderBytesOut) {
                COEFFICIENTS.put(ModelCoefficient.LEADER_BYTES_OUT, parameters[leaderBytesOutIndex]);
            }
            COEFFICIENTS.put(ModelCoefficient.FOLLOWER_BYTES_IN, parameters[followerBytesInIndex]);

            LOG.info("Coefficient generated: leader_bytes_in: {}, leader_bytes_out: {}, follower_bytes_in: {}",
                    COEFFICIENTS.get(ModelCoefficient.LEADER_BYTES_IN),
                    COEFFICIENTS.get(ModelCoefficient.LEADER_BYTES_OUT),
                    COEFFICIENTS.get(ModelCoefficient.FOLLOWER_BYTES_IN));
            return true;
        } catch (Exception e) {
            LOG.warn("received exception ", e);
        }
        return false;
    }

    /**
     * Add metric observation with the given training data.
     *
     * @param trainingData Training data.
     */
    public synchronized void addMetricObservation(Collection<BrokerMetricSample> trainingData) {
        if (trainingData != null) {
            for (BrokerMetricSample data : trainingData) {
                int utilBucket = (int) (data.metricValue(CPU_USAGE) / cpuUtilBucketSize);
                int index =
                        INDICES.computeIfAbsent(utilBucket, k -> new AtomicInteger(0)).getAndIncrement() % numObservationsPerUtilBucket;
                double[][] byteRateObservations =
                        BYTE_RATE_OBSERVATIONS.computeIfAbsent(utilBucket, k -> new double[numObservationsPerUtilBucket][]);
                double[] cpuUtilObservation =
                        CPU_UTIL_OBSERVATIONS.computeIfAbsent(utilBucket, k -> new double[numObservationsPerUtilBucket]);
                byteRateObservations[index] =
                        new double[]{data.metricValue(LEADER_BYTES_IN), data.metricValue(LEADER_BYTES_OUT), data.metricValue(REPLICATION_BYTES_IN_RATE)};
                cpuUtilObservation[index] = data.metricValue(CPU_USAGE);
                int leaderToFollowerBytesInRatio
                        = data.metricValue(REPLICATION_BYTES_IN_RATE) == 0.0
                        ? 10000000 : (int) ((data.metricValue(LEADER_BYTES_IN) / data.metricValue(REPLICATION_BYTES_IN_RATE)) * 10);
                int leaderBytesInToBytesOutRatio
                        = data.metricValue(LEADER_BYTES_OUT) == 0.0
                        ? 10000000 : (int) ((data.metricValue(LEADER_BYTES_IN) / data.metricValue(LEADER_BYTES_OUT)) * 10);
                int count = OBSERVED_LEADER_TO_FOLLOWER_BYTES_RATIO.getOrDefault(leaderToFollowerBytesInRatio, 0);
                OBSERVED_LEADER_TO_FOLLOWER_BYTES_RATIO.put(leaderToFollowerBytesInRatio, count + 1);
                count = OBSERVED_LEADER_BYTES_IN_TO_BYTES_OUT_RATIO.getOrDefault(leaderBytesInToBytesOutRatio, 0);
                OBSERVED_LEADER_BYTES_IN_TO_BYTES_OUT_RATIO.put(leaderBytesInToBytesOutRatio, count + 1);
                if (!COEFFICIENTS.isEmpty()) {
                    Double estimatedCpu = data.metricValue(LEADER_BYTES_IN) * COEFFICIENTS.get(ModelCoefficient.LEADER_BYTES_IN)
                            + data.metricValue(LEADER_BYTES_OUT) * COEFFICIENTS.getOrDefault(ModelCoefficient.LEADER_BYTES_OUT, 0.0)
                            + data.metricValue(REPLICATION_BYTES_IN_RATE) * COEFFICIENTS.get(ModelCoefficient.FOLLOWER_BYTES_IN);
                    int error = estimatedCpu.intValue() - data.metricValue(CPU_USAGE).intValue();
                    count = CPU_UTIL_ESTIMATION_ERROR_STATS.getOrDefault(error, 0);
                    CPU_UTIL_ESTIMATION_ERROR_STATS.put(error, count + 1);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("CPU util estimation: actual: {}, estimated: {}, error: {}",
                                data.metricValue(CPU_USAGE), estimatedCpu, estimatedCpu - data.metricValue(CPU_USAGE));
                    }
                }
            }
        }
    }

    /**
     * @return Model coefficient training completeness.
     */
    public double modelCoefficientTrainingCompleteness() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Linear regression model training data indices: {}", INDICES);
        }
        PriorityQueue<Integer> mostFilledBuckets =
                new PriorityQueue<>(minCpuUtilObservationBuckets);
        for (AtomicInteger index : INDICES.values()) {
            mostFilledBuckets.add(index.get());
            if (mostFilledBuckets.size() > minCpuUtilObservationBuckets) {
                mostFilledBuckets.remove();
            }
        }

        double completeness = 0.0;
        for (Integer index : mostFilledBuckets) {
            completeness += ((double) Math.min(index, numObservationsPerUtilBucket)) / numObservationsPerUtilBucket
                    / minCpuUtilObservationBuckets;
        }
        return completeness;
    }

    /**
     * @return Linear regression model state.
     */
    @Nonnull
    public synchronized LinearRegressionModelState modelState() {
        Map<Integer, Double> detailCompleteness = new HashMap<>();
        for (Map.Entry<Integer, AtomicInteger> entry : INDICES.entrySet()) {
            detailCompleteness.put(entry.getKey(),
                    Math.min((double) entry.getValue().get() / numObservationsPerUtilBucket, 1.0));
        }
        Map<Integer, Integer> usedLeaderToFollowerRatio = new HashMap<>();
        Map<Integer, Integer> usedLeaderBytesInToBytesOutRatio = new HashMap<>();
        Map<ModelCoefficient, Double> coefficientFromAvailableData = new HashMap<>(COEFFICIENTS);
        OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
        regression.setNoIntercept(true);
        boolean ignoreLeaderBytesOutRate = isLeaderBytesInAndOutRatioDiverseEnough();
        double[][] sampleBytesRateData = aggregateSampleBytesRateData(ignoreLeaderBytesOutRate);

        int leaderBytesInIndex = 0;
        int leaderBytesOutIndex = 1;
        int followerBytesInIndex = ignoreLeaderBytesOutRate ? 1 : 2;
        for (double[] sampleBytesRateDatum : sampleBytesRateData) {
            int leaderToFollowerRatio = sampleBytesRateDatum[followerBytesInIndex] == 0.0 ? 10000000 : (int) (
                    (sampleBytesRateDatum[leaderBytesInIndex] / sampleBytesRateDatum[followerBytesInIndex]) * 10);
            int count = usedLeaderToFollowerRatio.getOrDefault(leaderToFollowerRatio, 0);
            usedLeaderToFollowerRatio.put(leaderToFollowerRatio, count + 1);

            if (!ignoreLeaderBytesOutRate) {
                int leaderBytesInToBytesOutRatio = sampleBytesRateDatum[leaderBytesOutIndex] == 0.0 ? 10000000 : (int) (
                        (sampleBytesRateDatum[leaderBytesInIndex] / sampleBytesRateDatum[leaderBytesOutIndex]) * 10);
                count = usedLeaderBytesInToBytesOutRatio.getOrDefault(leaderBytesInToBytesOutRatio, 0);
                usedLeaderBytesInToBytesOutRatio.put(leaderBytesInToBytesOutRatio, count + 1);
            }
        }
        regression.newSampleData(aggregateSampleCpuUtilData(), sampleBytesRateData);
        double[] parameters = regression.estimateRegressionParameters();
        coefficientFromAvailableData.put(ModelCoefficient.LEADER_BYTES_IN, parameters[leaderBytesInIndex]);
        if (!ignoreLeaderBytesOutRate) {
            coefficientFromAvailableData.put(ModelCoefficient.LEADER_BYTES_OUT, parameters[leaderBytesOutIndex]);
        }
        coefficientFromAvailableData.put(ModelCoefficient.FOLLOWER_BYTES_IN, parameters[followerBytesInIndex]);
        return new LinearRegressionModelState(detailCompleteness, coefficientFromAvailableData,
                OBSERVED_LEADER_TO_FOLLOWER_BYTES_RATIO,
                OBSERVED_LEADER_BYTES_IN_TO_BYTES_OUT_RATIO,
                usedLeaderToFollowerRatio, usedLeaderBytesInToBytesOutRatio,
                CPU_UTIL_ESTIMATION_ERROR_STATS);
    }

    private Set<Integer> validBuckets() {
        Set<Integer> validBuckets = new HashSet<>();
        for (Map.Entry<Integer, AtomicInteger> entry : INDICES.entrySet()) {
            if (entry.getValue().get() >= numObservationsPerUtilBucket) {
                validBuckets.add(entry.getKey());
            }
        }
        return validBuckets;
    }

    private boolean isLeaderBytesInAndOutRatioDiverseEnough() {
        if (BYTE_RATE_OBSERVATIONS.isEmpty()) {
            return true;
        }
        long totalSamples = 0;
        Map<Integer, Integer> leaderForFollowerRatioHist = new HashMap<>();
        for (Map.Entry<Integer, double[][]> entry : BYTE_RATE_OBSERVATIONS.entrySet()) {
            int samplesInBucket = Math.min(numObservationsPerUtilBucket, INDICES.get(entry.getKey()).get());
            totalSamples += samplesInBucket;
            for (int i = 0; i < samplesInBucket; i++) {
                int leaderBytesInToFollowerRatio = entry.getValue()[i][1] == 0.0 ? 10000000 : (int) ((entry.getValue()[i][0] / entry.getValue()[i][1]) * 10);
                int count = leaderForFollowerRatioHist.getOrDefault(leaderBytesInToFollowerRatio, 0);
                leaderForFollowerRatioHist.put(leaderBytesInToFollowerRatio, count + 1);
            }
        }

        for (Integer count : leaderForFollowerRatioHist.values()) {
            if ((double) count / totalSamples > LEADER_BYTES_IN_AND_OUT_DIVERSITY_THRESHOLD) {
                LOG.info("Not enough diversity. {}", leaderForFollowerRatioHist);
                return true;
            }
        }
        LOG.info("Enough diversity.");
        return false;
    }

    private double[][] aggregateSampleBytesRateData(boolean ignoreLeaderBytesOutRate) {
        double[][] aggregatedSampleData = new double[numSamples()][];
        int indexForAggregatedData = 0;
        for (Map.Entry<Integer, double[][]> entry : BYTE_RATE_OBSERVATIONS.entrySet()) {
            int utilBucket = entry.getKey();
            double[][] sampleData = entry.getValue();
            for (int i = 0; i < Math.min(numObservationsPerUtilBucket, INDICES.get(utilBucket).get()); i++) {
                if (ignoreLeaderBytesOutRate) {
                    aggregatedSampleData[indexForAggregatedData] = new double[2];
                    aggregatedSampleData[indexForAggregatedData][0] = sampleData[i][0];
                    aggregatedSampleData[indexForAggregatedData][1] = sampleData[i][2];
                } else {
                    aggregatedSampleData[indexForAggregatedData] = sampleData[i];
                }
                indexForAggregatedData++;
            }
        }
        return aggregatedSampleData;
    }

    private double[] aggregateSampleCpuUtilData() {
        double[] aggregatedSampleData = new double[numSamples()];
        int indexForAggregatedData = 0;
        for (Map.Entry<Integer, double[]> entry : CPU_UTIL_OBSERVATIONS.entrySet()) {
            int utilBucket = entry.getKey();
            double[] sampleData = entry.getValue();
            for (int i = 0; i < Math.min(numObservationsPerUtilBucket, INDICES.get(utilBucket).get()); i++) {
                aggregatedSampleData[indexForAggregatedData] = sampleData[i];
                indexForAggregatedData++;
            }
        }
        return aggregatedSampleData;
    }

    private int numSamples() {
        int numSamples = 0;
        for (Integer utilBucket : CPU_UTIL_OBSERVATIONS.keySet()) {
            numSamples += Math.min(numObservationsPerUtilBucket, INDICES.get(utilBucket).get());
        }
        return numSamples;
    }

    /**
     * An enumeration holding the coefficients.
     */
    public enum ModelCoefficient {
        LEADER_BYTES_IN, LEADER_BYTES_OUT, FOLLOWER_BYTES_IN
    }

    public static class LinearRegressionModelState {
        private final Map<Integer, Double> _trainingState;
        private final Map<ModelCoefficient, Double> _modelCoefficients;
        private final Map<Integer, Integer> _observedLeaderToFollowerRatio;
        private final Map<Integer, Integer> _observedLeaderBytesInToBytesOutRatio;
        private final Map<Integer, Integer> _usedLeaderToFollowerRatio;
        private final Map<Integer, Integer> _usedLeaderBytesInToBytesOutRatio;
        private final Map<Integer, Integer> _estimatedCpuUtilErrorStats;

        LinearRegressionModelState(Map<Integer, Double> trainingState,
                                   Map<ModelCoefficient, Double> coefficients,
                                   Map<Integer, Integer> observedLeaderToFollowerRatio,
                                   Map<Integer, Integer> observedLeaderBytesInToBytesOutRatio,
                                   Map<Integer, Integer> usedLeaderToFollowerRatio,
                                   Map<Integer, Integer> usedLeaderBytesInToBytesOutRatio,
                                   Map<Integer, Integer> estimatedCpuUtilErrorStats) {
            _trainingState = trainingState;
            _modelCoefficients = coefficients;
            _observedLeaderToFollowerRatio = observedLeaderToFollowerRatio;
            _observedLeaderBytesInToBytesOutRatio = observedLeaderBytesInToBytesOutRatio;
            _usedLeaderToFollowerRatio = usedLeaderToFollowerRatio;
            _usedLeaderBytesInToBytesOutRatio = usedLeaderBytesInToBytesOutRatio;
            _estimatedCpuUtilErrorStats = estimatedCpuUtilErrorStats;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("TrainingState: \n{\n");
            for (Map.Entry<Integer, Double> entry : _trainingState.entrySet()) {
                builder.append(String.format("\t%3d%% - %3d%%: %.3f%n",
                        entry.getKey() * cpuUtilBucketSize,
                        Math.min((entry.getKey() + 1) * cpuUtilBucketSize, 100),
                        entry.getValue()));
            }
            builder.append("}\n\n");
            appendRatioHistogram(builder, "Observed leader to follower bytes in ratio", _observedLeaderToFollowerRatio);
            appendRatioHistogram(builder, "Observed leader bytes in to bytes out ratio", _observedLeaderBytesInToBytesOutRatio);
            appendRatioHistogram(builder, "Used leader to follower bytes in ratio", _usedLeaderToFollowerRatio);
            appendRatioHistogram(builder, "Used leader bytes in to bytes out ratio", _usedLeaderBytesInToBytesOutRatio);
            appendRatioHistogram(builder, "CPU estimation errors", _estimatedCpuUtilErrorStats);

            builder.append("Coefficients from available samples: \n")
                    .append(String.format("\t%20s: %.10f%n",
                            ModelCoefficient.LEADER_BYTES_IN,
                            _modelCoefficients.get(ModelCoefficient.LEADER_BYTES_IN)));
            if (_modelCoefficients.containsKey(ModelCoefficient.LEADER_BYTES_OUT)) {
                builder.append(String.format("\t%20s: %.10f%n",
                        ModelCoefficient.LEADER_BYTES_OUT,
                        _modelCoefficients.get(ModelCoefficient.LEADER_BYTES_OUT)));
            }
            builder.append(String.format("\t%20s: %.10f%n",
                            ModelCoefficient.FOLLOWER_BYTES_IN,
                            _modelCoefficients.get(ModelCoefficient.FOLLOWER_BYTES_IN)))
                    .append("\n");
            return builder.toString();
        }

        private void appendRatioHistogram(StringBuilder builder, String title, Map<Integer, Integer> ratioMap) {
            if (!ratioMap.isEmpty()) {
                builder.append(title).append(":\n{\n");

                for (Map.Entry<Integer, Integer> entry : ratioMap.entrySet()) {
                    builder.append(String.format("\t%20.2f: %8d%n", (double) entry.getKey() / 10, entry.getValue()));
                }

                builder.append("}\n\n");
            }
        }
    }
}
