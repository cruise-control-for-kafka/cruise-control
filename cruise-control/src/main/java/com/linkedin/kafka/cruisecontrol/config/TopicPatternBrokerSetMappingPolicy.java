/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.ReplicaToBrokerSetMappingException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;


/**
 * This replica to broker set mapping policy maps the replicas of a topic to the broker set whose {@code topicPattern} fully matches
 * the topic name, and leaves the replicas of topics that match no pattern unconstrained (i.e. returns {@code null} for them).
 * This lets operators pin selected topics to a broker set while all other topics stay free to move anywhere.
 *
 * <p>The patterns are read from the optional {@code topicPattern} field of the broker sets in {@code broker.set.config.file}:</p>
 * <pre>
 * {
 *   "brokerSets":[
 *     {
 *       "brokerSetId": "10G",
 *       "brokerIds": [2, 3],
 *       "topicPattern": "hot|orders-.*"
 *     }
 *   ]
 * }
 * </pre>
 *
 * <p>The file is read at most once per optimization (i.e. per {@link BrokerSetResolutionHelper}), so changes to it take effect
 * on the next optimization without a restart. The optimization fails if a pattern does not compile, if a topic matches the
 * patterns of more than one broker set, or if a topic matches the pattern of a broker set that the broker set resolver does not
 * know.</p>
 */
public class TopicPatternBrokerSetMappingPolicy implements ReplicaToBrokerSetMappingPolicy, CruiseControlConfigurable {
  private String _configFile;
  private volatile Snapshot _snapshot;

  @Override
  public void configure(Map<String, ?> configs) {
    Object configFile = configs.get(AnalyzerConfig.BROKER_SET_CONFIG_FILE_CONFIG);
    _configFile = configFile == null ? AnalyzerConfig.DEFAULT_BROKER_SET_CONFIG_FILE : configFile.toString();
  }

  /**
   * Maps a replica to the broker set whose topic pattern fully matches the replica's topic.
   *
   * @param replica replica object
   * @param clusterModel cluster model object
   * @param brokerSetResolutionHelper broker set resolution helper
   * @return A broker set Id, or {@code null} if no topic pattern matches the replica's topic
   */
  @Override
  public String brokerSetIdForReplica(final Replica replica, final ClusterModel clusterModel,
                                      final BrokerSetResolutionHelper brokerSetResolutionHelper) throws ReplicaToBrokerSetMappingException {
    return snapshotFor(brokerSetResolutionHelper).brokerSetIdForTopic(replica.topicPartition().topic());
  }

  /**
   * Each optimization creates a new {@link BrokerSetResolutionHelper}, so the topic patterns are reloaded whenever a different
   * helper arrives.
   *
   * @param brokerSetResolutionHelper broker set resolution helper
   * @return The snapshot of topic patterns for the given helper.
   */
  private Snapshot snapshotFor(BrokerSetResolutionHelper brokerSetResolutionHelper) {
    Snapshot snapshot = _snapshot;
    if (snapshot == null || snapshot._brokerSetResolutionHelper != brokerSetResolutionHelper) {
      synchronized (this) {
        snapshot = _snapshot;
        if (snapshot == null || snapshot._brokerSetResolutionHelper != brokerSetResolutionHelper) {
          snapshot = loadSnapshot(brokerSetResolutionHelper);
          _snapshot = snapshot;
        }
      }
    }
    return snapshot;
  }

  private Snapshot loadSnapshot(BrokerSetResolutionHelper brokerSetResolutionHelper) {
    BrokerSets brokerSets;
    try (Reader reader = Files.newBufferedReader(Path.of(_configFile), StandardCharsets.UTF_8)) {
      brokerSets = new Gson().fromJson(reader, BrokerSets.class);
    } catch (IOException | JsonParseException e) {
      return new Snapshot(brokerSetResolutionHelper, Collections.emptyMap(),
                          String.format("Failed to read topic patterns from %s: %s", _configFile, e.getMessage()));
    }

    // Sorted for deterministic matching order and error messages.
    Map<String, Pattern> topicPatternByBrokerSetId = new TreeMap<>();
    if (brokerSets != null && brokerSets.brokerSets != null) {
      for (BrokerSet brokerSet : brokerSets.brokerSets) {
        if (brokerSet == null || brokerSet.topicPattern == null || brokerSet.topicPattern.isEmpty()) {
          continue;
        }
        if (brokerSet.brokerSetId == null) {
          return new Snapshot(brokerSetResolutionHelper, Collections.emptyMap(),
                              String.format("Topic pattern %s in %s has no brokerSetId.", brokerSet.topicPattern, _configFile));
        }
        if (topicPatternByBrokerSetId.containsKey(brokerSet.brokerSetId)) {
          return new Snapshot(brokerSetResolutionHelper, Collections.emptyMap(),
                              String.format("BrokerSet %s defines topicPattern more than once in %s.", brokerSet.brokerSetId, _configFile));
        }
        try {
          topicPatternByBrokerSetId.put(brokerSet.brokerSetId, Pattern.compile(brokerSet.topicPattern));
        } catch (PatternSyntaxException e) {
          return new Snapshot(brokerSetResolutionHelper, Collections.emptyMap(),
                              String.format("Invalid topicPattern %s of brokerSet %s in %s: %s", brokerSet.topicPattern,
                                            brokerSet.brokerSetId, _configFile, e.getDescription()));
        }
      }
    }
    return new Snapshot(brokerSetResolutionHelper, topicPatternByBrokerSetId, null);
  }

  /**
   * An immutable view of the topic patterns for one {@link BrokerSetResolutionHelper}, with a cache of topic to broker set answers.
   */
  private static final class Snapshot {
    private final BrokerSetResolutionHelper _brokerSetResolutionHelper;
    private final Map<String, Pattern> _topicPatternByBrokerSetId;
    private final String _loadError;
    private final Map<String, Optional<String>> _brokerSetIdByTopic = new ConcurrentHashMap<>();

    private Snapshot(BrokerSetResolutionHelper brokerSetResolutionHelper, Map<String, Pattern> topicPatternByBrokerSetId,
                     String loadError) {
      _brokerSetResolutionHelper = brokerSetResolutionHelper;
      _topicPatternByBrokerSetId = Collections.unmodifiableMap(topicPatternByBrokerSetId);
      _loadError = loadError;
    }

    private String brokerSetIdForTopic(String topic) throws ReplicaToBrokerSetMappingException {
      if (_loadError != null) {
        throw new ReplicaToBrokerSetMappingException(String.format("Cannot map topic %s to a broker set. %s", topic, _loadError));
      }
      Optional<String> cached = _brokerSetIdByTopic.get(topic);
      if (cached != null) {
        return cached.orElse(null);
      }

      List<String> matchedBrokerSetIds = new ArrayList<>();
      for (Map.Entry<String, Pattern> entry : _topicPatternByBrokerSetId.entrySet()) {
        if (entry.getValue().matcher(topic).matches()) {
          matchedBrokerSetIds.add(entry.getKey());
        }
      }
      if (matchedBrokerSetIds.size() > 1) {
        throw new ReplicaToBrokerSetMappingException(
            String.format("Topic %s matches the topic patterns of more than one brokerSet %s (topic patterns: %s).", topic,
                          matchedBrokerSetIds, patternsOf(matchedBrokerSetIds)));
      }
      String brokerSetId = matchedBrokerSetIds.isEmpty() ? null : matchedBrokerSetIds.get(0);
      Set<String> knownBrokerSetIds = _brokerSetResolutionHelper.brokersByBrokerSetId().keySet();
      if (brokerSetId != null && !knownBrokerSetIds.contains(brokerSetId)) {
        throw new ReplicaToBrokerSetMappingException(
            String.format("Topic %s matches the topic pattern of brokerSet %s (topic patterns: %s), which is not a known brokerSet %s.",
                          topic, brokerSetId, patternsOf(matchedBrokerSetIds), knownBrokerSetIds));
      }
      _brokerSetIdByTopic.put(topic, Optional.ofNullable(brokerSetId));
      return brokerSetId;
    }

    private Map<String, String> patternsOf(List<String> brokerSetIds) {
      Map<String, String> patterns = new TreeMap<>();
      brokerSetIds.forEach(id -> patterns.put(id, _topicPatternByBrokerSetId.get(id).pattern()));
      return patterns;
    }
  }

  private static class BrokerSets {
    private List<BrokerSet> brokerSets;
  }

  private static class BrokerSet {
    private final String brokerSetId;
    private final String topicPattern;

    BrokerSet(String brokerSetId, String topicPattern) {
      this.brokerSetId = brokerSetId;
      this.topicPattern = topicPattern;
    }
  }
}
