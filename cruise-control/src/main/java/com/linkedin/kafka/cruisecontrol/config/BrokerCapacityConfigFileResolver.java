/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The broker capacity config resolver implementation based on files. The format of the file is JSON. Depending on
 * whether the JBOD configuration is used or not, this JSON file may specify disk capacity per logDir.
 * Example capacity file without JBOD:
 * <pre>
 *   {
 *      "brokerCapacities":[
 *        {
 *          "brokerId": "-1",
 *          "capacity": {
 *            "DISK": "1000000",
 *            "CPU": "100",
 *            "NW_IN": "100000",
 *            "NW_OUT": "100000"
 *          }
 *        },
 *        {
 *          "brokerId": "0",
 *          "capacity": {
 *            "DISK": "1000000",
 *            "CPU": "100",
 *            "NW_IN": "100000",
 *            "NW_OUT": "100000"
 *          }
 *        },
 *        {
 *          "brokerId": "1",
 *          "capacity": {
 *            "DISK": "1000000",
 *            "CPU": "100",
 *            "NW_IN": "100000",
 *            "NW_OUT": "100000"
 *          }
 *        }
 *      ]
 *   }
 * </pre>
 *
 * Example capacity file with JBOD (provided logDirs must be absolute paths):
 * <pre>
 *   {
 *      "brokerCapacities":[
 *        {
 *          "brokerId": "-1",
 *          "capacity": {
 *            "DISK": {"/tmp/kafka-logs-1": "400000", "/tmp/kafka-logs-2": "200000", "/tmp/kafka-logs-3": "200000",
 *           "/tmp/kafka-logs-4": "200000", "/tmp/kafka-logs-5": "200000", "/tmp/kafka-logs-6": "200000"},
 *            "CPU": "100",
 *            "NW_IN": "100000",
 *            "NW_OUT": "100000"
 *          }
 *        },
 *        {
 *          "brokerId": "0",
 *          "capacity": {
 *            "DISK": {"/tmp/kafka-logs-1": "350000", "/tmp/kafka-logs-2": "550000"},
 *            "CPU": "100",
 *            "NW_IN": "100000",
 *            "NW_OUT": "100000"
 *          }
 *        },
 *        {
 *          "brokerId": "1",
 *          "capacity": {
 *            "DISK": {"/tmp/kafka-logs": "2000000"},
 *            "CPU": "100",
 *            "NW_IN": "100000",
 *            "NW_OUT": "100000"
 *          }
 *        }
 *      ]
 *   }
 * </pre>
 *
 * Example capacity file with number of cores (i.e. see the use of {@link #NUM_CORES_CONFIG} config):
 * <pre>
 * {
 *   "brokerCapacities":[
 *     {
 *       "brokerId": "-1",
 *       "capacity": {
 *         "DISK": {"/tmp/kafka-logs-1": "100000", "/tmp/kafka-logs-2": "100000", "/tmp/kafka-logs-3": "50000",
 *           "/tmp/kafka-logs-4": "50000", "/tmp/kafka-logs-5": "150000", "/tmp/kafka-logs-6": "50000"},
 *         "CPU": {"num.cores": "16"},
 *         "NW_IN": "10000",
 *         "NW_OUT": "10000"
 *       },
 *       "doc": "The default capacity for a broker with multiple logDirs each on a separate heterogeneous disk and num.cores CPU cores."
 *     },
 *     {
 *       "brokerId": "0",
 *       "capacity": {
 *         "DISK": {"/tmp/kafka-logs": "500000"},
 *         "CPU": {"num.cores": "32"},
 *         "NW_IN": "50000",
 *         "NW_OUT": "50000"
 *       },
 *       "doc": "This overrides the capacity for broker 0. This broker is not a JBOD broker, but has num.cores CPU cores."
 *     },
 *     {
 *       "brokerId": "1",
 *       "capacity": {
 *         "DISK": {"/tmp/kafka-logs-1": "250000", "/tmp/kafka-logs-2": "250000"},
 *         "CPU": {"num.cores": "24"},
 *         "NW_IN": "50000",
 *         "NW_OUT": "50000"
 *       },
 *       "doc": "This overrides the capacity for broker 1. This broker is a JBOD broker and has num.cores CPU cores."
 *     }
 *   ]
 * }
 * </pre>
 *
 * The broker id -1 defines the default broker capacity estimate. A broker capacity is overridden if there is a capacity
 * defined for a particular broker id. In case a broker capacity is missing, the default estimate for a broker capacity
 * will be used.
 *
 * The units of the definition are:
 * <ul>
 *  <li>DISK - MB</li>
 *  <li>CPU - either (1) Percentage (0 - 100) or (2) number of cores (see the use of {@link #NUM_CORES_CONFIG} config) </li>
 *  <li>NW_IN - KB/s</li>
 *  <li>NW_OUT - KB/s</li>
 * </ul>
 */
public class BrokerCapacityConfigFileResolver implements BrokerCapacityConfigResolver {
  public static final String CAPACITY_CONFIG_FILE = "capacity.config.file";
  public static final int DEFAULT_CAPACITY_BROKER_ID = -1;
  private static final String NUM_CORES_CONFIG = "num.cores";
  public static final double DEFAULT_CPU_CAPACITY_WITH_CORES = 100.0;
  private static Map<Integer, BrokerCapacityInfo> capacitiesForBrokers;
  private String _configFile;
  private static final Logger LOG = LoggerFactory.getLogger(BrokerCapacityConfigFileResolver.class);

  @Override
  public void configure(Map<String, ?> configs) {
    _configFile = KafkaCruiseControlUtils.getRequiredConfig(configs, CAPACITY_CONFIG_FILE);
    try {
      loadCapacities();
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public BrokerCapacityInfo capacityForBroker(String rack, String host, int brokerId, long timeoutMs, boolean allowCapacityEstimation)
      throws BrokerCapacityResolutionException {
    if (brokerId >= 0) {
      BrokerCapacityInfo capacity = capacitiesForBrokers.get(brokerId);
      if (capacity != null) {
        return capacity;
      } else {
        if (allowCapacityEstimation) {
          String info = String.format("Missing broker id(%d) in capacity config file.", brokerId);
          return new BrokerCapacityInfo(capacitiesForBrokers.get(DEFAULT_CAPACITY_BROKER_ID).capacity(), info,
                                        capacitiesForBrokers.get(DEFAULT_CAPACITY_BROKER_ID).diskCapacityByLogDir(),
                                        capacitiesForBrokers.get(DEFAULT_CAPACITY_BROKER_ID).numCpuCores());
        } else {
          throw new BrokerCapacityResolutionException(String.format("Unable to resolve capacity of broker %d. Either (1) adding the "
              + "default broker capacity (via adding capacity for broker %d and allowing capacity estimation) or (2) adding missing "
              + "broker's capacity in file %s.", brokerId, DEFAULT_CAPACITY_BROKER_ID, _configFile));
        }
      }
    } else {
      throw new IllegalArgumentException("The broker id(" + brokerId + ") should be non-negative.");
    }
  }

  @Override
  public boolean isJbodKafkaCluster() {
    // If any of the brokers in the cluster are using JBOD, the cluster is considered to be using JBOD
    for (Map.Entry<Integer, BrokerCapacityInfo> entry : capacitiesForBrokers.entrySet()) {
      if (entry.getValue().diskCapacityByLogDir() != null && entry.getValue().diskCapacityByLogDir().size() > 1) {
        LOG.info("Kafka Cluster is considered as a JBOD type");
        return true;
      }
    }
    LOG.info("Kafka Cluster is considered as a non-JBOD type");
    return false;
  }

  private static boolean isJBOD(Map<Resource, Object> brokerCapacity) {
    return brokerCapacity.get(Resource.DISK) instanceof Map;
  }

  /**
   * Get the number of cores specified by user -- i.e. requires the {@link Resource#CPU} capacity to specify
   * {@link #NUM_CORES_CONFIG} in a Map.
   *
   * @param brokerCapacity Broker capacity for each resource.
   * @return Number of cores if specified by user, {@code null} otherwise.
   */
  @SuppressWarnings("unchecked")
  private static Double getUserSpecifiedNumCores(Map<Resource, Object> brokerCapacity) {
    if (brokerCapacity.get(Resource.CPU) instanceof Map) {
      String stringNumCores = ((Map<String, String>) brokerCapacity.get(Resource.CPU)).get(NUM_CORES_CONFIG);
      if (stringNumCores == null) {
        throw new IllegalArgumentException("Missing " + NUM_CORES_CONFIG + " config for brokers in capacity config file.");
      }
      return Double.parseDouble(stringNumCores);
    } else {
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<Resource, Double> getTotalCapacity(Map<Resource, Object> brokerCapacity, boolean hasNumCores) {
    Map<Resource, Double> totalCapacity = new HashMap<>();
    if (isJBOD(brokerCapacity)) {
      for (Map.Entry<Resource, Object> entry : brokerCapacity.entrySet()) {
        Resource resource = entry.getKey();
        if (resource == Resource.DISK) {
          double totalDiskCapacity = 0.0;
          for (Map.Entry<String, String> diskEntry : ((Map<String, String>) brokerCapacity.get(resource)).entrySet()) {
            if (!Paths.get(diskEntry.getKey()).isAbsolute()) {
              throw new IllegalArgumentException("The logDir " + diskEntry.getKey() + " must be an absolute path.");
            }

            totalDiskCapacity += Double.parseDouble(diskEntry.getValue());
          }
          totalCapacity.put(resource, totalDiskCapacity);
        } else if (hasNumCores && resource == Resource.CPU) {
          totalCapacity.put(resource, DEFAULT_CPU_CAPACITY_WITH_CORES);
        } else {
          totalCapacity.put(resource, Double.parseDouble((String) entry.getValue()));
        }
      }
    } else {
      brokerCapacity.forEach((key, value) -> totalCapacity.put(key, hasNumCores && key == Resource.CPU ? DEFAULT_CPU_CAPACITY_WITH_CORES
                                                                                                       : Double.parseDouble((String) value)));
    }

    return totalCapacity;
  }

  /**
   * Get disk capacity by absolute logDir path if the capacity is specified per logDir, null otherwise.
   *
   * @param brokerCapacity Broker capacity for each resource.
   * @return Disk capacity by absolute logDir path if the capacity is specified per logDir, null otherwise.
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Double> getDiskCapacityByLogDir(Map<Resource, Object> brokerCapacity) {
    if (!isJBOD(brokerCapacity)) {
      return null;
    }

    Map<String, String> stringDiskCapacityByLogDir = (Map<String, String>) brokerCapacity.get(Resource.DISK);
    Map<String, Double> diskCapacityByLogDir = new HashMap<>();
    stringDiskCapacityByLogDir.forEach((key, value) -> diskCapacityByLogDir.put(key, Double.parseDouble(value)));

    return diskCapacityByLogDir;
  }

  private static void numCoresConfigConsistencyChecker(Set<Boolean> numCoresConfigConsistency) {
    if (numCoresConfigConsistency.size() > 1) {
      throw new IllegalArgumentException("Inconsistent " + NUM_CORES_CONFIG + " config for brokers in capacity config file. This "
                                         + "config must be provided by either all or non of the brokers.");
    }
  }

  private BrokerCapacityInfo getBrokerCapacityInfo(BrokerCapacity bc, Set<Boolean> numCoresConfigConsistency) {
    Double userSpecifiedNumCores = getUserSpecifiedNumCores(bc.capacity);
    boolean hasNumCores = userSpecifiedNumCores != null;
    numCoresConfigConsistency.add(hasNumCores);
    numCoresConfigConsistencyChecker(numCoresConfigConsistency);
    boolean isDefault = bc.brokerId == DEFAULT_CAPACITY_BROKER_ID;
    Map<Resource, Double> totalCapacity = getTotalCapacity(bc.capacity, hasNumCores);
    Map<String, Double> diskCapacityByLogDir = getDiskCapacityByLogDir(bc.capacity);

    BrokerCapacityInfo brokerCapacityInfo;
    if (hasNumCores) {
      brokerCapacityInfo
          = isDefault ? new BrokerCapacityInfo(totalCapacity, "The default broker capacity.", diskCapacityByLogDir, userSpecifiedNumCores)
                      : new BrokerCapacityInfo(totalCapacity, diskCapacityByLogDir, userSpecifiedNumCores);
    } else {
      brokerCapacityInfo
          = isDefault ? new BrokerCapacityInfo(totalCapacity, "The default broker capacity.", diskCapacityByLogDir)
                      : new BrokerCapacityInfo(totalCapacity, diskCapacityByLogDir);
    }
    return brokerCapacityInfo;
  }

  private void loadCapacities() throws FileNotFoundException {
    JsonReader reader = null;
    try {
      reader = new JsonReader(new InputStreamReader(new FileInputStream(_configFile), StandardCharsets.UTF_8));
      Gson gson = new Gson();
      Set<BrokerCapacity> brokerCapacities = ((BrokerCapacities) gson.fromJson(reader, BrokerCapacities.class)).brokerCapacities;
      capacitiesForBrokers = new HashMap<>();
      Set<Boolean> numCoresConfigConsistency = new HashSet<>();
      if (brokerCapacities != null) {
        for (BrokerCapacity bc : brokerCapacities) {
          capacitiesForBrokers.put(bc.brokerId, getBrokerCapacityInfo(bc, numCoresConfigConsistency));
        }
      }
    } finally {
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        // let it go.
      }
    }
  }

  @Override
  public void close() {
    // nothing to do.
  }

  private static class BrokerCapacities {
    private Set<BrokerCapacity> brokerCapacities;
  }

  private static class BrokerCapacity {
    private final int brokerId;
    private final Map<Resource, Object> capacity;

    BrokerCapacity(int brokerId, Map<Resource, Object> capacity) {
      this.brokerId = brokerId;
      this.capacity = capacity;
    }
  }
}
