/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;


/**
 * Provision recommendation to add (if {@link ProvisionStatus#UNDER_PROVISIONED}) or remove (if {@link ProvisionStatus#OVER_PROVISIONED})
 * resources to/from the cluster with the given constraints.
 *
 * <p>Resources include:
 * <ul>
 *   <li>Brokers that can host replicas</li>
 *   <li>Racks containing brokers that can host replicas</li>
 *   <li>Disks (i.e. relevant to JBOD deployments)</li>
 *   <li>Partitions of a topic (can only be added)</li>
 * </ul>
 *
 * <p>Constraints include:
 * <ul>
 *   <li>Topic name regex: If the resource is partition, the regex name of the topic must be specified.</li>
 *   <li>A typical broker id and its capacity (one cannot be specified without the other)</li>
 *   <li>Specific resource, such as {@link Resource#DISK}.</li>
 *   <li>Excluded racks -- i.e. racks for which brokers should not be added to or removed from</li>
 *   <li>Total resource capacity required to add or remove</li>
 * </ul>
 */
public final class ProvisionRecommendation {
  public static final int DEFAULT_OPTIONAL_INT = -1;
  public static final double DEFAULT_OPTIONAL_DOUBLE = -1.0;
  // Provisions status to identify whether resources will be added or removed.
  private final ProvisionStatus _status;
  // Number of resources recommended to be added or removed. For number of partitions, the value indicates the target number of partitions.
  private final int _numBrokers;
  private final int _numRacks;
  private final int _numDisks;
  private final int _numPartitions;
  // If the resource is partition, the regex name of the topic must be specified.
  private final Pattern _topicPattern;
  // A typical broker id and its capacity (one cannot be specified without the other)
  private final int _typicalBrokerId;
  private final double _typicalBrokerCapacity;
  // Specific resource, such as {@link Resource#DISK}
  private final Resource _resource;
  // Excluded racks -- i.e. racks for which brokers should not be added to or removed from
  private final Set<String> _excludedRackIds;
  // Total resource capacity required to add or remove
  private final double _totalCapacity;

  public static class Builder {
    // Required parameters
    private final ProvisionStatus _status;
    // Optional parameters - initialized to default values
    private int _numBrokers = DEFAULT_OPTIONAL_INT;
    private int _numRacks = DEFAULT_OPTIONAL_INT;
    private int _numDisks = DEFAULT_OPTIONAL_INT;
    private int _numPartitions = DEFAULT_OPTIONAL_INT;
    private Pattern _topicPattern = null;
    private int _typicalBrokerId = DEFAULT_OPTIONAL_INT;
    private double _typicalBrokerCapacity = DEFAULT_OPTIONAL_DOUBLE;
    private Resource _resource = null;
    private Set<String> _excludedRackIds = null;
    private double _totalCapacity = DEFAULT_OPTIONAL_DOUBLE;

    public Builder(ProvisionStatus status) {
      if (!(status == ProvisionStatus.UNDER_PROVISIONED || status == ProvisionStatus.OVER_PROVISIONED)) {
        throw new IllegalArgumentException(String.format("Provision recommendation is irrelevant for provision status %s.", status));
      }
      _status = status;
    }

    /**
     * (Optional) Set number of brokers
     * @param numBrokers Number of brokers to add or remove
     * @return this builder.
     */
    public Builder numBrokers(int numBrokers) {
      if (numBrokers <= 0) {
        throw new IllegalArgumentException(String.format("Number of brokers must be positive (%d).", numBrokers));
      }
      _numBrokers = numBrokers;
      return this;
    }

    /**
     * (Optional) Set number of racks
     * @param numRacks Number of racks to add or remove
     * @return this builder.
     */
    public Builder numRacks(int numRacks) {
      if (numRacks <= 0) {
        throw new IllegalArgumentException(String.format("Number of racks must be positive (%d).", numRacks));
      }
      _numRacks = numRacks;
      return this;
    }

    /**
     * (Optional) Set number of disks
     * @param numDisks Number of disks to add or remove
     * @return this builder.
     */
    public Builder numDisks(int numDisks) {
      if (numDisks <= 0) {
        throw new IllegalArgumentException(String.format("Number of disks must be positive (%d).", numDisks));
      }
      _numDisks = numDisks;
      return this;
    }

    /**
     * (Optional) Set number of partitions
     * @param numPartitions The recommended number of partitions.
     * @return this builder.
     */
    public Builder numPartitions(int numPartitions) {
      if (numPartitions <= 0) {
        throw new IllegalArgumentException(String.format("Number of partitions must be positive (%d).", numPartitions));
      }
      _numPartitions = numPartitions;
      return this;
    }

    /**
     * (Optional) Set the topic name regex
     * @param topicPattern Topic name regex for which to add partitions
     * @return this builder.
     */
    public Builder topicPattern(Pattern topicPattern) {
      if (topicPattern == null || topicPattern.pattern().isEmpty()) {
        throw new IllegalArgumentException("The regex name of the topic must be specified");
      }
      _topicPattern = topicPattern;
      return this;
    }

    /**
     * (Optional) Set the typical broker id
     * @param typicalBrokerId The typical broker id
     * @return this builder.
     */
    public Builder typicalBrokerId(int typicalBrokerId) {
      if (typicalBrokerId < 0) {
        throw new IllegalArgumentException(String.format("Typical broker id must be non-negative (%d).", typicalBrokerId));
      }
      _typicalBrokerId = typicalBrokerId;
      return this;
    }

    /**
     * (Optional) Set the typical broker capacity
     * @param typicalBrokerCapacity The typical broker capacity
     * @return this builder.
     */
    public Builder typicalBrokerCapacity(double typicalBrokerCapacity) {
      if (typicalBrokerCapacity <= 0.0) {
        throw new IllegalArgumentException(String.format("Typical broker capacity must be positive (%f).", typicalBrokerCapacity));
      }
      _typicalBrokerCapacity = typicalBrokerCapacity;
      return this;
    }

    /**
     * (Optional) Set the resource
     * @param resource The resource
     * @return this builder.
     */
    public Builder resource(Resource resource) {
      _resource = resource;
      return this;
    }

    /**
     * (Optional) Set the excluded rack ids
     * @param excludedRackIds The resource
     * @return this builder.
     */
    public Builder excludedRackIds(Set<String> excludedRackIds) {
      if (excludedRackIds == null || excludedRackIds.isEmpty()) {
        throw new IllegalArgumentException("Provided ids for excluded racks cannot be null or empty.");
      }

      for (String rackId : excludedRackIds) {
        if (rackId.isEmpty()) {
          throw new IllegalArgumentException("Excluded rack ids cannot contain an empty rack id.");
        }
      }

      _excludedRackIds = excludedRackIds;
      return this;
    }

    /**
     * (Optional) Set the total capacity
     * @param totalCapacity Total capacity
     * @return this builder.
     */
    public Builder totalCapacity(double totalCapacity) {
      if (totalCapacity <= 0.0) {
        throw new IllegalArgumentException(String.format("Total capacity must be positive (%f).", totalCapacity));
      }
      _totalCapacity = totalCapacity;
      return this;
    }

    public ProvisionRecommendation build() {
      return new ProvisionRecommendation(this);
    }
  }

  private ProvisionRecommendation(Builder builder) {
    sanityCheckResources(builder);
    sanityCheckTypical(builder);
    sanityCheckExcludedRackIds(builder);
    _status = builder._status;
    _numBrokers = builder._numBrokers;
    _numRacks = builder._numRacks;
    _numDisks = builder._numDisks;
    _numPartitions = builder._numPartitions;
    _topicPattern = builder._topicPattern;
    _typicalBrokerId = builder._typicalBrokerId;
    _typicalBrokerCapacity = builder._typicalBrokerCapacity;
    _resource = builder._resource;
    _excludedRackIds = builder._excludedRackIds;
    _totalCapacity = builder._totalCapacity;
  }

  /**
   * Package private for unit test.
   * Ensure that
   * <ul>
   *   <li>exactly one resource type is set</li>
   *   <li>if the resource type is partition, then the cluster is under provisioned</li>
   *   <li>if the resource type is partition, then the corresponding topic regex must be specified; otherwise, the topic regex must be omitted</li>
   * </ul>
   *
   * @param builder The builder for sanity check upon construction
   */
  static void sanityCheckResources(Builder builder) {
    int numSetResources = builder._numBrokers != DEFAULT_OPTIONAL_INT ? 1 : 0;
    if (builder._numRacks != DEFAULT_OPTIONAL_INT) {
      numSetResources++;
    }
    if (builder._numDisks != DEFAULT_OPTIONAL_INT) {
      numSetResources++;
    }
    if (builder._numPartitions != DEFAULT_OPTIONAL_INT) {
      numSetResources++;
    }

    if (numSetResources != 1) {
      throw new IllegalArgumentException(
          String.format("Exactly one resource type must be set (Brokers:%s Racks:%s Disks:%s Partitions:%s).",
                        builder._numBrokers == DEFAULT_OPTIONAL_INT ? "-" : String.valueOf(builder._numBrokers),
                        builder._numRacks == DEFAULT_OPTIONAL_INT ? "-" : String.valueOf(builder._numRacks),
                        builder._numDisks == DEFAULT_OPTIONAL_INT ? "-" : String.valueOf(builder._numDisks),
                        builder._numPartitions == DEFAULT_OPTIONAL_INT ? "-" : String.valueOf(builder._numPartitions)));
    }

    if (builder._numPartitions != DEFAULT_OPTIONAL_INT) {
      if (builder._status != ProvisionStatus.UNDER_PROVISIONED) {
        throw new IllegalArgumentException("When the resource type is partition, the cluster must be under provisioned.");
      } else if (builder._topicPattern == null) {
        throw new IllegalArgumentException("When the resource type is partition, the corresponding topic regex must be specified.");
      }
    } else if (builder._topicPattern != null) {
      throw new IllegalArgumentException("When the resource type is not partition, topic regex cannot be specified.");
    }
  }

  /**
   * Package private for unit test.
   * Ensure that
   * <ul>
   *   <li>a typical broker id or its capacity is not specified without the other one.</li>
   *   <li>if typical capacity and id are specified, then number of brokers is also specified.</li>
   *   <li>if typical capacity and id are specified, then resource is also specified.</li>
   * </ul>
   * @param builder The builder for sanity check upon construction
   */
  static void sanityCheckTypical(Builder builder) {
    int numSetTypical = builder._typicalBrokerId != DEFAULT_OPTIONAL_INT ? 1 : 0;
    if (builder._typicalBrokerCapacity != DEFAULT_OPTIONAL_DOUBLE) {
      numSetTypical++;
    }

    if (numSetTypical == 1) {
      throw new IllegalArgumentException(
          String.format("Typical broker id must be specified with its capacity (Id:%s Capacity:%s).",
                        builder._typicalBrokerId == DEFAULT_OPTIONAL_INT ? "-" : String.valueOf(builder._typicalBrokerId),
                        builder._typicalBrokerCapacity == DEFAULT_OPTIONAL_DOUBLE ? "-" : String.valueOf(builder._typicalBrokerCapacity)));
    } else if (numSetTypical == 2) {
      if (builder._numBrokers == DEFAULT_OPTIONAL_INT) {
        throw new IllegalArgumentException("Typical broker id and capacity cannot be specified without number of brokers.");
      } else if (builder._resource == null) {
        throw new IllegalArgumentException("Typical broker id and capacity cannot be specified without the resource.");
      }
    }
  }

  /**
   * Package private for unit test.
   * Ensure that
   * <ul>
   *   <li>excluded rack ids can be specified only with the number of brokers</li>
   * </ul>
   * @param builder The builder for sanity check upon construction
   */
  static void sanityCheckExcludedRackIds(Builder builder) {
    if (builder._excludedRackIds != null && builder._numBrokers == DEFAULT_OPTIONAL_INT) {
      throw new IllegalArgumentException("Excluded rack ids can be specified only with the number of brokers.");
    }
  }

  public ProvisionStatus status() {
    return _status;
  }

  public int numBrokers() {
    return _numBrokers;
  }

  public int numRacks() {
    return _numRacks;
  }

  public int numDisks() {
    return _numDisks;
  }

  public int numPartitions() {
    return _numPartitions;
  }

  public Pattern topicPattern() {
    return _topicPattern;
  }

  public int typicalBrokerId() {
    return _typicalBrokerId;
  }

  public double typicalBrokerCapacity() {
    return _typicalBrokerCapacity;
  }

  public Resource resource() {
    return _resource;
  }

  public Set<String> excludedRackIds() {
    return _excludedRackIds == null ? null : Collections.unmodifiableSet(_excludedRackIds);
  }

  public double totalCapacity() {
    return _totalCapacity;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    // 1. Add or remove resources
    sb.append(String.format("%s at least ", _status == ProvisionStatus.UNDER_PROVISIONED ? "Add" : "Remove"));
    // 2. Specify the number of brokers, racks, disks, or partitions
    if (_numBrokers != DEFAULT_OPTIONAL_INT) {
      sb.append(String.format("%d broker%s", _numBrokers, _numBrokers > 1 ? "s" : ""));
    } else if (_numRacks != DEFAULT_OPTIONAL_INT) {
      sb.append(String.format("%d rack%s with brokers", _numRacks, _numRacks > 1 ? "s" : ""));
    } else if (_numDisks != DEFAULT_OPTIONAL_INT) {
      sb.append(String.format("%d disk%s", _numDisks, _numDisks > 1 ? "s" : ""));
    } else if (_numPartitions != DEFAULT_OPTIONAL_INT) {
      sb.append(String.format("the minimum number of partitions so that the topic regex %s has %d partition%s",
                              _topicPattern, _numPartitions, _numPartitions > 1 ? "s" : ""));
    }
    // 3. (optional) Typical broker id, its capacity, and resource
    if (_typicalBrokerId != DEFAULT_OPTIONAL_INT) {
      sb.append(String.format(" with the same %s capacity (%.2f) as broker-%d", _resource, _typicalBrokerCapacity, _typicalBrokerId));
    } else if (_resource != null) {
      sb.append(String.format(" for %s", _resource));
    }
    // 4. (optional) Excluded rack ids
    if (_excludedRackIds != null) {
      sb.append(String.format(" %s a rack other than %s", _status == ProvisionStatus.UNDER_PROVISIONED ? "to" : "from", _excludedRackIds));
    }
    // 5. (optional) Total capacity
    if (_totalCapacity != DEFAULT_OPTIONAL_DOUBLE) {
      sb.append(String.format(" with a total capacity of %.2f", _totalCapacity));
    }

    // 6. End with a dot
    return sb.append(".").toString();
  }
}
