/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.linkedin.cruisecontrol.CruiseControlUtils.currentUtcDate;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.ADD_BROKER;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.DEMOTE_BROKER;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.FIX_OFFLINE_REPLICAS;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.REVIEW;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.GET_METHOD;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.POST_METHOD;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.getClientIpAddress;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters.TopicConfigurationType.REPLICATION_FACTOR;
import static com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus.APPROVED;
import static com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus.DISCARDED;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.writeErrorResponse;
import static jakarta.servlet.http.HttpServletResponse.SC_BAD_REQUEST;

/**
 * The util class for Kafka Cruise Control parameters.
 */
public final class ParameterUtils {
  public static final String JSON_PARAM = "json";
  public static final String GET_RESPONSE_SCHEMA = "get_response_schema";
  public static final String START_MS_PARAM = "start";
  public static final String END_MS_PARAM = "end";
  public static final String ENTRIES_PARAM = "entries";
  public static final String ALLOW_CAPACITY_ESTIMATION_PARAM = "allow_capacity_estimation";
  public static final String STOP_ONGOING_EXECUTION_PARAM = "stop_ongoing_execution";
  public static final String CLEAR_METRICS_PARAM = "clearmetrics";
  public static final String TIME_PARAM = "time";
  public static final String VERBOSE_PARAM = "verbose";
  public static final String SUPER_VERBOSE_PARAM = "super_verbose";
  public static final String RESOURCE_PARAM = "resource";
  public static final String REASON_PARAM = "reason";
  public static final String DATA_FROM_PARAM = "data_from";
  public static final String KAFKA_ASSIGNER_MODE_PARAM = "kafka_assigner";
  public static final String MAX_LOAD_PARAM = "max_load";
  public static final String AVG_LOAD_PARAM = "avg_load";
  public static final String GOALS_PARAM = "goals";
  public static final String BROKER_ID_PARAM = "brokerid";
  public static final String DROP_RECENTLY_REMOVED_BROKERS_PARAM = "drop_recently_removed_brokers";
  public static final String DROP_RECENTLY_DEMOTED_BROKERS_PARAM = "drop_recently_demoted_brokers";
  public static final String DESTINATION_BROKER_IDS_PARAM = "destination_broker_ids";
  public static final String REVIEW_ID_PARAM = "review_id";
  public static final String REVIEW_IDS_PARAM = "review_ids";
  public static final String TOPIC_PARAM = "topic";
  public static final String PARTITION_PARAM = "partition";
  public static final String DRY_RUN_PARAM = "dryrun";
  public static final String THROTTLE_ADDED_BROKER_PARAM = "throttle_added_broker";
  public static final String THROTTLE_REMOVED_BROKER_PARAM = "throttle_removed_broker";
  public static final String REPLICATION_THROTTLE_PARAM = "replication_throttle";
  public static final String IGNORE_PROPOSAL_CACHE_PARAM = "ignore_proposal_cache";
  public static final String USE_READY_DEFAULT_GOALS_PARAM = "use_ready_default_goals";
  public static final String EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM = "execution_progress_check_interval_ms";
  public static final String CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM = "concurrent_partition_movements_per_broker";
  public static final String MAX_PARTITION_MOVEMENTS_IN_CLUSTER_PARAM = "max_partition_movements_in_cluster";
  public static final String CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PARAM = "concurrent_intra_broker_partition_movements";
  public static final String CONCURRENT_LEADER_MOVEMENTS_PARAM = "concurrent_leader_movements";
  public static final String BROKER_CONCURRENT_LEADER_MOVEMENTS_PARAM = "broker_concurrent_leader_movements";
  public static final String DEFAULT_PARTITION_LOAD_RESOURCE = "disk";
  public static final String SUBSTATES_PARAM = "substates";
  public static final String MIN_VALID_PARTITION_RATIO_PARAM = "min_valid_partition_ratio";
  public static final String SKIP_HARD_GOAL_CHECK_PARAM = "skip_hard_goal_check";
  public static final String EXCLUDED_TOPICS_PARAM = "excluded_topics";
  public static final String USER_TASK_IDS_PARAM = "user_task_ids";
  public static final String CLIENT_IDS_PARAM = "client_ids";
  public static final String ENDPOINTS_PARAM = "endpoints";
  public static final String TYPES_PARAM = "types";
  public static final String SKIP_URP_DEMOTION_PARAM = "skip_urp_demotion";
  public static final String EXCLUDE_FOLLOWER_DEMOTION_PARAM = "exclude_follower_demotion";
  public static final String DISABLE_SELF_HEALING_FOR_PARAM = "disable_self_healing_for";
  public static final String ENABLE_SELF_HEALING_FOR_PARAM = "enable_self_healing_for";
  public static final String DISABLE_CONCURRENCY_ADJUSTER_FOR_PARAM = "disable_concurrency_adjuster_for";
  public static final String ENABLE_CONCURRENCY_ADJUSTER_FOR_PARAM = "enable_concurrency_adjuster_for";
  public static final String MIN_ISR_BASED_CONCURRENCY_ADJUSTMENT_PARAM = "min_isr_based_concurrency_adjustment";
  public static final String EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM = "exclude_recently_demoted_brokers";
  public static final String EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM = "exclude_recently_removed_brokers";
  public static final String REPLICA_MOVEMENT_STRATEGIES_PARAM = "replica_movement_strategies";
  public static final String APPROVE_PARAM = "approve";
  public static final String DISCARD_PARAM = "discard";
  public static final String REBALANCE_DISK_MODE_PARAM = "rebalance_disk";
  public static final String POPULATE_DISK_INFO_PARAM = "populate_disk_info";
  public static final String CAPACITY_ONLY_PARAM = "capacity_only";
  public static final String BROKER_ID_AND_LOGDIRS_PARAM = "brokerid_and_logdirs";
  public static final String REPLICATION_FACTOR_PARAM = "replication_factor";
  public static final String SKIP_RACK_AWARENESS_CHECK_PARAM = "skip_rack_awareness_check";
  public static final String FETCH_COMPLETED_TASK_PARAM = "fetch_completed_task";
  public static final String FORCE_STOP_PARAM = "force_stop";
  public static final String FAST_MODE_PARAM = "fast_mode";
  public static final String STOP_EXTERNAL_AGENT_PARAM = "stop_external_agent";
  public static final String DEVELOPER_MODE_PARAM = "developer_mode";
  private static final int MAX_REASON_LENGTH = 50;
  private static final String DELIMITER_BETWEEN_BROKER_ID_AND_LOGDIR = "-";
  public static final long DEFAULT_START_TIME_FOR_CLUSTER_MODEL = -1L;
  public static final String TOPIC_BY_REPLICATION_FACTOR = "topic_by_replication_factor";
  public static final String NO_REASON_PROVIDED = "No reason provided";
  public static final String DO_AS = "doAs";
  public static final String NUM_BROKERS_TO_ADD = "num_brokers_to_add";
  public static final String PARTITION_COUNT = "partition_count";

  public static final String STOP_PROPOSAL_PARAMETER_OBJECT_CONFIG = "stop.proposal.parameter.object";
  public static final String BOOTSTRAP_PARAMETER_OBJECT_CONFIG = "bootstrap.parameter.object";
  public static final String TRAIN_PARAMETER_OBJECT_CONFIG = "train.parameter.object";
  public static final String LOAD_PARAMETER_OBJECT_CONFIG = "load.parameter.object";
  public static final String PARTITION_LOAD_PARAMETER_OBJECT_CONFIG = "partition.load.parameter.object";
  public static final String PROPOSALS_PARAMETER_OBJECT_CONFIG = "proposals.parameter.object";
  public static final String STATE_PARAMETER_OBJECT_CONFIG = "state.parameter.object";
  public static final String KAFKA_CLUSTER_STATE_PARAMETER_OBJECT_CONFIG = "kafka.cluster.state.parameter.object";
  public static final String USER_TASKS_PARAMETER_OBJECT_CONFIG = "user.tasks.parameter.object";
  public static final String REVIEW_BOARD_PARAMETER_OBJECT_CONFIG = "review.board.parameter.object";
  public static final String ADD_BROKER_PARAMETER_OBJECT_CONFIG = "add.broker.parameter.object";
  public static final String REMOVE_BROKER_PARAMETER_OBJECT_CONFIG = "remove.broker.parameter.object";
  public static final String FIX_OFFLINE_REPLICAS_PARAMETER_OBJECT_CONFIG = "fix.offline.replicas.parameter.object";
  public static final String REBALANCE_PARAMETER_OBJECT_CONFIG = "rebalance.parameter.object";
  public static final String PAUSE_RESUME_PARAMETER_OBJECT_CONFIG = "pause.resume.parameter.object";
  public static final String DEMOTE_BROKER_PARAMETER_OBJECT_CONFIG = "demote.broker.parameter.object";
  public static final String ADMIN_PARAMETER_OBJECT_CONFIG = "admin.parameter.object";
  public static final String REVIEW_PARAMETER_OBJECT_CONFIG = "review.parameter.object";
  public static final String TOPIC_CONFIGURATION_PARAMETER_OBJECT_CONFIG = "topic.configuration.parameter.object";
  public static final String RIGHTSIZE_PARAMETER_OBJECT_CONFIG = "rightsize.parameter.object";
  public static final String PERMISSIONS_PARAMETER_OBJECT_CONFIG = "permissions.parameter.object";
  public static final String REMOVE_DISKS_PARAMETER_OBJECT_CONFIG = "remove.disks.parameter.object";

  private ParameterUtils() {
  }

  /**
   * @param requestContext The Http request.
   * @return The endpoint specified in the given request.
   */
  public static CruiseControlEndPoint endPoint(CruiseControlRequestContext requestContext) {
    List<CruiseControlEndPoint> supportedEndpoints;
    switch (requestContext.getMethod()) {
      case GET_METHOD:
        supportedEndpoints = CruiseControlEndPoint.getEndpoints();
        break;
      case POST_METHOD:
        supportedEndpoints = CruiseControlEndPoint.postEndpoints();
        break;
      default:
        throw new UserRequestException("Unsupported request method: " + requestContext.getMethod() + ".");
    }
    String pathInfo = requestContext.getPathInfo();
    if (pathInfo == null) {
      // URL does not have any extra path information
      return null;
    }
    // Skip the first character '/'
    Path path = Path.of(pathInfo).getFileName();
    for (CruiseControlEndPoint endPoint : supportedEndpoints) {
      if (endPoint.toString().equalsIgnoreCase(String.valueOf(path))) {
        return endPoint;
      }
    }
    return null;
  }

  static void handleParameterParseException(Exception e,
                                            CruiseControlRequestContext requestContext,
                                            String errorMessage,
                                            boolean json,
                                            boolean wantJsonSchema) throws IOException {
    writeErrorResponse(requestContext, e, errorMessage, SC_BAD_REQUEST, json, wantJsonSchema);
  }

  /**
   * Check whether the request has valid parameter names. If not, populate the HTTP response with the corresponding
   * error message and return {@code false}, return {@code true} otherwise.
   *
   * @param requestContext the request context.
   * @param parameters Request parameters
   * @return {@code true} if the request has valid parameter names, {@code false} otherwise (and response is populated).
   */
  public static boolean hasValidParameterNames(CruiseControlRequestContext requestContext,
                                               CruiseControlParameters parameters) throws IOException {
    CruiseControlEndPoint endPoint = endPoint(requestContext);
    Set<String> validParamNames = parameters.caseInsensitiveParameterNames();
    Set<String> userParams = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    userParams.addAll(requestContext.getParameterMap().keySet());
    if (validParamNames != null) {
      userParams.removeAll(validParamNames);
    }

    if (!userParams.isEmpty()) {
      // User request specifies parameters that are not a subset of the valid parameters.
      String errorMessage = String.format("Unrecognized endpoint parameters in %s %s request: %s.",
              endPoint, requestContext.getMethod(), userParams);
      writeErrorResponse(requestContext, null, errorMessage, SC_BAD_REQUEST, wantJSON(requestContext), wantResponseSchema(requestContext));
      return false;
    }
    return true;
  }

  /**
   * @param parameterMap Parameter map
   * @param parameter Parameter to parse from the parameter map.
   * @return The case sensitive request parameter name, or <code>null</code> if the parameter does not exist.
   */
  public static String caseSensitiveParameterName(Map<String, String[]> parameterMap, String parameter) {
    return parameterMap.keySet().stream().filter(parameter::equalsIgnoreCase).findFirst().orElse(null);
  }

  /**
   * Get the boolean parameter.
   *
   * @param requestContext the request context.
   * @param parameter Parameter to parse from the request.
   * @param defaultIfMissing Default value to set if the request does not contain the parameter.
   * @return The specified value for the parameter, or defaultIfMissing if the parameter is missing.
   */
  public static boolean getBooleanParam(CruiseControlRequestContext requestContext, String parameter, boolean defaultIfMissing) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), parameter);
    return parameterString == null ? defaultIfMissing : Boolean.parseBoolean(requestContext.getParameter(parameterString));
  }

  /**
   * Get the long parameter parameter.
   *
   * @param requestContext HTTP request received by Cruise Control.
   * @param parameter Parameter to parse from the request.
   * @param defaultIfMissing Default value to set if the request does not contain the parameter.
   * @return The specified value for the parameter, or defaultIfMissing if the parameter is missing.
   */
  public static Long getLongParam(CruiseControlRequestContext requestContext, String parameter, @Nullable Long defaultIfMissing) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), parameter);
    return parameterString == null ? defaultIfMissing : Long.valueOf(requestContext.getParameter(parameterString));
  }

  /**
   * Get the {@link List} parameter.
   *
   * @param requestContext the request context.
   * @param parameter Parameter to parse from the request.
   * @return The specified value for the parameter, or empty List if the parameter is missing.
   */
  public static List<String> getListParam(CruiseControlRequestContext requestContext, String parameter) throws UnsupportedEncodingException {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), parameter);
    List<String> retList = parameterString == null ? new ArrayList<>()
            : Arrays.asList(urlDecode(requestContext.getParameter(parameterString)).split(","));
    retList.removeIf(String::isEmpty);
    return Collections.unmodifiableList(retList);
  }

  /**
   * Get the {@link List} parameter.
   * @param requestContext the request context.
   * @return The specified value for the parameter, or empty List if the parameter is missing.
   */

  public static boolean wantJSON(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, JSON_PARAM, false);
  }

  public static boolean wantResponseSchema(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, GET_RESPONSE_SCHEMA, false);
  }

  static boolean allowCapacityEstimation(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, ALLOW_CAPACITY_ESTIMATION_PARAM, true);
  }

  static boolean skipRackAwarenessCheck(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, SKIP_RACK_AWARENESS_CHECK_PARAM, false);
  }

  static boolean stopOngoingExecution(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, STOP_ONGOING_EXECUTION_PARAM, false);
  }

  private static boolean excludeBrokers(CruiseControlRequestContext requestContext, String parameter, boolean defaultIfMissing) {
    boolean isKafkaAssignerMode = isKafkaAssignerMode(requestContext);
    boolean excludeBrokers = getBooleanParam(requestContext, parameter, defaultIfMissing);
    if (isKafkaAssignerMode && excludeBrokers) {
      throw new UserRequestException("Kafka assigner mode does not support excluding brokers.");
    }

    return excludeBrokers;
  }

  private static boolean getBooleanExcludeGiven(CruiseControlRequestContext requestContext, String getParameter, Set<String> excludeParameters) {
    boolean booleanParam = getBooleanParam(requestContext, getParameter, false);
    if (booleanParam) {
      for (String excludeParameter : excludeParameters) {
        if (caseSensitiveParameterName(requestContext.getParameterMap(), excludeParameter) != null) {
          throw new UserRequestException("Cannot set " + getParameter + " parameter to true when explicitly specifying "
                                         + excludeParameter + " in the request.");
        }
      }
    }
    return booleanParam;
  }

  /**
   * Default: {@code false} -- i.e. recently demoted brokers may receive leadership from the other brokers as long as
   * {@link ExecutorConfig#DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG}
   * @param requestContext the request context.
   * @return The value of {@link #EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM} parameter.
   */
  static boolean excludeRecentlyDemotedBrokers(CruiseControlRequestContext requestContext) {
    return excludeBrokers(requestContext, EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM, false);
  }

  /**
   * Default: {@code true} -- i.e. recently removed brokers may not receive replicas from the other brokers as long as
   * {@link ExecutorConfig#REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG}.
   * @param requestContext the request context.
   * @return The value of {@link #EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM} parameter.
   */
  static boolean excludeRecentlyRemovedBrokers(CruiseControlRequestContext requestContext) {
    return excludeBrokers(requestContext, EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM, true);
  }

  static boolean wantMaxLoad(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, MAX_LOAD_PARAM, false);
  }

  static boolean wantAvgLoad(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, AVG_LOAD_PARAM, false);
  }

  static boolean isVerbose(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, VERBOSE_PARAM, false);
  }

  static boolean isSuperVerbose(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, SUPER_VERBOSE_PARAM, false);
  }

  static boolean clearMetrics(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, CLEAR_METRICS_PARAM, true);
  }

  private static boolean isKafkaAssignerMode(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, KAFKA_ASSIGNER_MODE_PARAM, false);
  }

  static boolean isRebalanceDiskMode(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, REBALANCE_DISK_MODE_PARAM, false);
  }

  static boolean populateDiskInfo(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, POPULATE_DISK_INFO_PARAM, false);
  }

  static boolean capacityOnly(CruiseControlRequestContext requestContext) {
    Set<String> excludeParameters =
        Set.of(TIME_PARAM, END_MS_PARAM, START_MS_PARAM, ALLOW_CAPACITY_ESTIMATION_PARAM, POPULATE_DISK_INFO_PARAM);
    return getBooleanExcludeGiven(requestContext, CAPACITY_ONLY_PARAM, excludeParameters);
  }

  static boolean ignoreProposalCache(CruiseControlRequestContext requestContext) {
    return getBooleanExcludeGiven(requestContext, IGNORE_PROPOSAL_CACHE_PARAM, Collections.singleton(GOALS_PARAM));
  }

  static boolean useReadyDefaultGoals(CruiseControlRequestContext requestContext) {
    return getBooleanExcludeGiven(requestContext, USE_READY_DEFAULT_GOALS_PARAM, Collections.singleton(GOALS_PARAM));
  }

  static boolean fastMode(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, FAST_MODE_PARAM, true);
  }

  static boolean getDryRun(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, DRY_RUN_PARAM, true);
  }

  static boolean forceExecutionStop(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, FORCE_STOP_PARAM, false);
  }

  static boolean stopExternalAgent(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, STOP_EXTERNAL_AGENT_PARAM, true);
  }

  static boolean developerMode(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, DEVELOPER_MODE_PARAM, false);
  }

  static boolean throttleAddedOrRemovedBrokers(CruiseControlRequestContext requestContext, EndPoint endPoint) {
    return endPoint == ADD_BROKER ? getBooleanParam(requestContext, THROTTLE_ADDED_BROKER_PARAM, true)
            : getBooleanParam(requestContext, THROTTLE_REMOVED_BROKER_PARAM, true);
  }

  static Long replicationThrottle(CruiseControlRequestContext requestContext, KafkaCruiseControlConfig config) {
    Long value = getLongParam(requestContext, REPLICATION_THROTTLE_PARAM, config.getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG));
    if (value != null && value < 0) {
      throw new UserRequestException(String.format("Requested rebalance throttle must be non-negative (Requested: %s).", value));
    }
    return value;
  }

  static Long time(CruiseControlRequestContext requestContext) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), TIME_PARAM);
    if (parameterString == null) {
      return null;
    }

    if (caseSensitiveParameterName(requestContext.getParameterMap(), END_MS_PARAM) != null) {
      throw new UserRequestException(String.format("Parameter %s and parameter %s are mutually exclusive and should "
                                                   + "not be specified in the same request.", TIME_PARAM, END_MS_PARAM));
    }

    String timeString = requestContext.getParameter(parameterString);
    return "NOW".equalsIgnoreCase(timeString) ? System.currentTimeMillis() : Long.parseLong(timeString);
  }

  static Long startMsOrDefault(CruiseControlRequestContext controlRequestContext, @Nullable Long defaultIfMissing) {
    return getLongParam(controlRequestContext, START_MS_PARAM, defaultIfMissing);
  }

  static Long endMsOrDefault(CruiseControlRequestContext requestContext, @Nullable Long defaultIfMissing) {
    return getLongParam(requestContext, END_MS_PARAM, defaultIfMissing);
  }

  static void validateTimeRange(long startMs, long endMs) {
    if (startMs >= endMs) {
      throw new UserRequestException(
          String.format("Invalid time range. Start time must be smaller than end time. Got: [%d, %d]", startMs, endMs));
    }
  }

  static Pattern topic(CruiseControlRequestContext requestContext) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), TOPIC_PARAM);
    return parameterString == null ? null : Pattern.compile(requestContext.getParameter(parameterString));
  }

  @SuppressWarnings("unchecked")
  private static Map<Short, Pattern> topicPatternByReplicationFactorFromBody(CruiseControlRequestContext requestContext) {
    Map<Short, Pattern> topicPatternByReplicationFactor;
    try {
      Map<String, Object> json = requestContext.getJson();
      if (json == null) {
        return null;
      }
      String replicationFactorKey = REPLICATION_FACTOR.name().toLowerCase();
      if (!json.containsKey(replicationFactorKey)) {
        return null;
      }
      Map<String, Object> replicationFactorParams = (Map<String, Object>) json.get(replicationFactorKey);
      if (!replicationFactorParams.containsKey(TOPIC_BY_REPLICATION_FACTOR)) {
        return null;
      }
      topicPatternByReplicationFactor = new HashMap<>();
      for (Map.Entry<String, String> entry : ((Map<String, String>) replicationFactorParams.get(TOPIC_BY_REPLICATION_FACTOR)).entrySet()) {
        short replicationFactor = Short.parseShort(entry.getKey().trim());
        Pattern topicPattern = Pattern.compile(entry.getValue().trim());
        topicPatternByReplicationFactor.putIfAbsent(replicationFactor, topicPattern);
      }
    } catch (IOException ioe) {
      throw new UserRequestException(String.format("Illegal value for field %s in body, please specify in pairs of \"target replication "
                                                   + "factor\" : \"topic name regex\".", TOPIC_BY_REPLICATION_FACTOR));
    }
    return topicPatternByReplicationFactor;
  }

  static Map<Short, Pattern> topicPatternByReplicationFactor(CruiseControlRequestContext requestContext) {
    Pattern topic = topic(requestContext);
    Short replicationFactor = replicationFactor(requestContext);
    Map<Short, Pattern> topicPatternByReplicationFactorFromBody = topicPatternByReplicationFactorFromBody(requestContext);
    if (topicPatternByReplicationFactorFromBody != null) {
      if (topic != null || replicationFactor != null) {
        throw new UserRequestException("Requesting topic replication factor change from both HTTP request parameter and body"
                                       + " is forbidden.");
      }
      return topicPatternByReplicationFactorFromBody;
    } else {
      if (topic == null && replicationFactor != null) {
        throw new UserRequestException("Topic is not specified in URL while target replication factor is specified.");
      }
      if ((topic != null && replicationFactor == null)) {
        throw new UserRequestException("Topic's replication factor is not specified in URL while subject topic is specified.");
      }
      if (topic != null) {
        return Collections.singletonMap(replicationFactor, topic);
      }
    }
    return Collections.emptyMap();
  }

  static Double minValidPartitionRatio(CruiseControlRequestContext requestContext) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), MIN_VALID_PARTITION_RATIO_PARAM);
    if (parameterString == null) {
      return null;
    } else {
      double minValidPartitionRatio = Double.parseDouble(requestContext.getParameter(parameterString));
      if (minValidPartitionRatio > 1.0 || minValidPartitionRatio < 0.0) {
        throw new UserRequestException("The requested minimum partition ratio must be in range [0.0, 1.0] (Requested: "
                                       + minValidPartitionRatio + ").");
      }
      return minValidPartitionRatio;
    }
  }

  /**
   * Get the {@link #RESOURCE_PARAM} from the request.
   *
   * @param requestContext HTTP request received by Cruise Control.
   * @return The resource String from the request, or {@link #DEFAULT_PARTITION_LOAD_RESOURCE} if {@link #RESOURCE_PARAM}
   * does not exist in the request.
   */
  public static String resourceString(CruiseControlRequestContext requestContext) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), RESOURCE_PARAM);
    return parameterString == null ? DEFAULT_PARTITION_LOAD_RESOURCE : requestContext.getParameter(parameterString);
  }

  /**
   * Get the {@link #REASON_PARAM} from the request.
   *
   * @param requestContext HTTP request received by Cruise Control.
   * @param reasonRequired {@code true} if the {@link #REASON_PARAM} parameter is required, {@code false} otherwise.
   * @return The specified value for the {@link #REASON_PARAM} parameter, or {@link #NO_REASON_PROVIDED} if parameter
   * does not exist in the request.
   */
  public static String reason(CruiseControlRequestContext requestContext, boolean reasonRequired) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), REASON_PARAM);
    if (parameterString != null && parameterString.length() > MAX_REASON_LENGTH) {
      throw new UserRequestException(String.format("Reason cannot be longer than %d characters (attempted: %d).",
                                                   MAX_REASON_LENGTH, parameterString.length()));
    }
    String ip = getClientIpAddress(requestContext);
    if (parameterString == null && reasonRequired) {
      throw new UserRequestException("Reason is missing in request.");
    }
    return String.format("%s (Client: %s, Date: %s)", parameterString == null ? NO_REASON_PROVIDED
            : requestContext.getParameter(parameterString), ip, currentUtcDate());
  }

  /**
   * Parse the given parameter to a Set of String.
   *
   * @param requestContext HTTP request received by Cruise Control.
   * @param param The name of parameter.
   * @return Parsed parameter as a Set of String.
   */
  public static Set<String> parseParamToStringSet(CruiseControlRequestContext requestContext, String param) throws UnsupportedEncodingException {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), param);
    Set<String> paramsString = parameterString == null
                               ? new HashSet<>(0)
            : new HashSet<>(Arrays.asList(urlDecode(requestContext.getParameter(parameterString)).split(",")));
    paramsString.removeIf(String::isEmpty);
    return paramsString;
  }

  /**
   * Parse the given parameter to a Set of Integer.
   *
   * @param requestContext the request context.
   * @param param The name of parameter.
   * @return Parsed parameter as a Set of Integer.
   */
  public static Set<Integer> parseParamToIntegerSet(CruiseControlRequestContext requestContext, String param) throws UnsupportedEncodingException {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), param);

    return parameterString == null ? new HashSet<>(0)
            : Arrays.stream(urlDecode(requestContext.getParameter(parameterString)).split(","))
                                           .map(Integer::parseInt).collect(Collectors.toSet());
  }

  /**
   * Empty parameter means all substates are requested.
   * @param requestContext Http request.
   * @return The value of {@link #SUBSTATES_PARAM} parameter.
   */
  static Set<CruiseControlState.SubState> substates(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    Set<String> substatesString = parseParamToStringSet(requestContext, SUBSTATES_PARAM);

    Set<CruiseControlState.SubState> substates = new HashSet<>();
    try {
      for (String substateString : substatesString) {
        substates.add(CruiseControlState.SubState.valueOf(substateString.toUpperCase()));
      }
    } catch (IllegalArgumentException iae) {
      throw new UserRequestException(String.format("Unsupported substates in %s. Supported: %s",
                                                   substatesString, CruiseControlState.SubState.cachedValues()));
    }

    return Collections.unmodifiableSet(substates);
  }

  private static Set<AnomalyType> anomalyTypes(CruiseControlRequestContext requestContext, boolean isEnable) throws UnsupportedEncodingException {
    Set<String> selfHealingForString = parseParamToStringSet(requestContext, isEnable ? ENABLE_SELF_HEALING_FOR_PARAM
                                                                               : DISABLE_SELF_HEALING_FOR_PARAM);

    Set<AnomalyType> anomalyTypes = new HashSet<>();
    try {
      for (String shfString : selfHealingForString) {
        anomalyTypes.add(KafkaAnomalyType.valueOf(shfString.toUpperCase()));
      }
    } catch (IllegalArgumentException iae) {
      throw new UserRequestException(String.format("Unsupported anomaly types in %s. Supported: %s",
                                                   selfHealingForString, KafkaAnomalyType.cachedValues()));
    }

    return Collections.unmodifiableSet(anomalyTypes);
  }

  private static Set<ConcurrencyType> concurrencyTypes(CruiseControlRequestContext requestContext,
                                                       boolean isEnable) throws UnsupportedEncodingException {
    Set<String> concurrencyForStringSet = parseParamToStringSet(requestContext, isEnable ? ENABLE_CONCURRENCY_ADJUSTER_FOR_PARAM
                                                                                  : DISABLE_CONCURRENCY_ADJUSTER_FOR_PARAM);

    Set<ConcurrencyType> concurrencyTypes = new HashSet<>();
    try {
      for (String concurrencyForString : concurrencyForStringSet) {
        concurrencyTypes.add(ConcurrencyType.valueOf(concurrencyForString.toUpperCase()));
      }
    } catch (IllegalArgumentException iae) {
      throw new UserRequestException(String.format("Unsupported concurrency types in %s. Supported: %s",
                                                   concurrencyForStringSet, ConcurrencyType.cachedValues()));
    }

    return Collections.unmodifiableSet(concurrencyTypes);
  }

  /**
   * Get self healing types for {@link #ENABLE_SELF_HEALING_FOR_PARAM} and {@link #DISABLE_SELF_HEALING_FOR_PARAM}.
   *
   * Sanity check ensures that the same anomaly is not specified in both configs at the same request.
   * @param requestContext Http servlet request.
   * @return The self healing types for {@link #ENABLE_SELF_HEALING_FOR_PARAM} and {@link #DISABLE_SELF_HEALING_FOR_PARAM}.
   */
  static Map<Boolean, Set<AnomalyType>> selfHealingFor(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    Set<AnomalyType> enableSelfHealingFor = anomalyTypes(requestContext, true);
    Set<AnomalyType> disableSelfHealingFor = anomalyTypes(requestContext, false);

    // Sanity check: Ensure that the same anomaly is not specified in both configs at the same request.
    ensureDisjoint(enableSelfHealingFor, disableSelfHealingFor,
                   "The same anomaly cannot be specified in both disable and enable parameters");

    Map<Boolean, Set<AnomalyType>> selfHealingFor = Map.of(true, enableSelfHealingFor, false, disableSelfHealingFor);
    return selfHealingFor;
  }

  /**
   * Get concurrency adjuster types for {@link #ENABLE_CONCURRENCY_ADJUSTER_FOR_PARAM} and {@link #DISABLE_CONCURRENCY_ADJUSTER_FOR_PARAM}.
   *
   * Sanity check ensures that the same concurrency type is not specified in both configs at the same request.
   * @param requestContext Http request.
   * @return The concurrency adjuster types for {@link #ENABLE_CONCURRENCY_ADJUSTER_FOR_PARAM} and {@link #DISABLE_CONCURRENCY_ADJUSTER_FOR_PARAM}.
   */
  static Map<Boolean, Set<ConcurrencyType>> concurrencyAdjusterFor(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    Set<ConcurrencyType> enableConcurrencyAdjusterFor = concurrencyTypes(requestContext, true);
    Set<ConcurrencyType> disableConcurrencyAdjusterFor = concurrencyTypes(requestContext, false);

    // Sanity check: Ensure that the same concurrency type is not specified in both configs at the same request.
    ensureDisjoint(enableConcurrencyAdjusterFor, disableConcurrencyAdjusterFor,
                   "The same concurrency type cannot be specified in both disable and enable parameters");

    Map<Boolean, Set<ConcurrencyType>> concurrencyAdjusterFor = Map.of(true, enableConcurrencyAdjusterFor,
                                                                       false, disableConcurrencyAdjusterFor);
    return concurrencyAdjusterFor;
  }

  /**
   * @param requestContext The http request.
   * @return {@code true}: enable or {@code false}: disable MinISR-based concurrency adjustment, {@code null} if the request parameter is unset.
   */
  @Nullable
  static Boolean minIsrBasedConcurrencyAdjustment(CruiseControlRequestContext requestContext) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), MIN_ISR_BASED_CONCURRENCY_ADJUSTMENT_PARAM);
    if (parameterString == null) {
      return null;
    }
    return Boolean.parseBoolean(requestContext.getParameter(parameterString));
  }

  /**
   * Compare and ensure two sets are disjoint as part of a user request.
   * @param set1 The first set to compare.
   * @param set2 The second set to compare.
   * @param errorMessage The message to pass to {@link UserRequestException}if two sets are not disjoint.
   * @param <E> The type of elements maintained by the sets.
   */
  static <E> void ensureDisjoint(Set<E> set1, Set<E> set2, String errorMessage) {
    Set<E> intersection = new HashSet<>(set1);
    intersection.retainAll(set2);
    if (!intersection.isEmpty()) {
      throw new UserRequestException(String.format("%s. Intersection: %s.", errorMessage, intersection));
    }
  }

  static String urlDecode(String s) throws UnsupportedEncodingException {
    return s == null ? null : URLDecoder.decode(s, StandardCharsets.UTF_8.name());
  }

  static ReplicaMovementStrategy getReplicaMovementStrategy(CruiseControlRequestContext requestContext, KafkaCruiseControlConfig config)
      throws UnsupportedEncodingException {
    if (getDryRun(requestContext)) {
      return null;
    }
    List<String> strategies = getListParam(requestContext, REPLICA_MOVEMENT_STRATEGIES_PARAM);
    if (strategies.isEmpty()) {
      return null;
    }
    List<ReplicaMovementStrategy> supportedStrategies = config.getConfiguredInstances(ExecutorConfig.REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                                                                      ReplicaMovementStrategy.class);
    Map<String, ReplicaMovementStrategy> supportedStrategiesByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (ReplicaMovementStrategy strategy : supportedStrategies) {
      supportedStrategiesByName.put(strategy.name(), strategy);
    }
    ReplicaMovementStrategy strategy = null;
    for (String strategyName : strategies) {
      if (supportedStrategiesByName.containsKey(strategyName)) {
        strategy = strategy == null ? supportedStrategiesByName.get(strategyName) : strategy.chain(supportedStrategiesByName.get(strategyName));
      } else {
        throw new UserRequestException("Strategy " + strategyName + " is not supported. Supported: " + supportedStrategiesByName.keySet());
      }
    }
    return strategy.chainBaseReplicaMovementStrategyIfAbsent();
  }

  static List<String> getGoals(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    boolean isKafkaAssignerMode = isKafkaAssignerMode(requestContext);
    boolean isRebalanceDiskMode = isRebalanceDiskMode(requestContext);
    // Sanity check isKafkaAssignerMode and isRebalanceDiskMode are not both true at the same time.
    if (isKafkaAssignerMode && isRebalanceDiskMode) {
      throw new UserRequestException("Kafka assigner mode and rebalance disk mode cannot be set the at the same time.");
    }
    List<String> goals = getListParam(requestContext, GOALS_PARAM);

    // KafkaAssigner mode is assumed to use two KafkaAssigner goals, if client specifies goals in request, throw exception.
    if (isKafkaAssignerMode) {
      if (!goals.isEmpty()) {
        throw new UserRequestException("Kafka assigner mode does not support explicitly specifying goals in request.");
      }
      return Collections.unmodifiableList(Arrays.asList(KafkaAssignerEvenRackAwareGoal.class.getSimpleName(),
              KafkaAssignerDiskUsageDistributionGoal.class.getSimpleName()));
    }
    if (isRebalanceDiskMode) {
      if (!goals.isEmpty()) {
        throw new UserRequestException("Rebalance disk mode does not support explicitly specifying goals in request.");
      }
      return Collections.unmodifiableList(Arrays.asList(IntraBrokerDiskCapacityGoal.class.getSimpleName(),
              IntraBrokerDiskUsageDistributionGoal.class.getSimpleName()));
    }
    return goals;
  }

  /**
   * Get the specified value for the {@link #ENTRIES_PARAM} parameter.
   *
   * @param requestContext HTTP request received by Cruise Control.
   * @return The specified value for the {@link #ENTRIES_PARAM} parameter, or {@link Integer#MAX_VALUE} if the
   * parameter is missing.
   */
  public static int entries(CruiseControlRequestContext requestContext) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), ENTRIES_PARAM);
    int entries = parameterString == null ? Integer.MAX_VALUE : Integer.parseInt(requestContext.getParameter(parameterString));
    if (entries <= 0) {
      throw new UserRequestException("The requested entries must be positive (Requested: " + entries + ").");
    }
    return entries;
  }

  /**
   * @param values Integer values
   * @return A set of negative integer values contained in the given set.
   */
  private static Set<Integer> getNegatives(Set<Integer> values) {
    return values.stream().filter(v -> v < 0).collect(Collectors.toCollection(() -> new HashSet<>()));
  }

  /**
   * Default: An empty set.
   * @param requestContext the request context.
   * @return Review Ids.
   */
  public static Set<Integer> reviewIds(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    Set<Integer> reviewIds = parseParamToIntegerSet(requestContext, REVIEW_IDS_PARAM);
    Set<Integer> negativeReviewIds = getNegatives(reviewIds);
    if (!negativeReviewIds.isEmpty()) {
      throw new UserRequestException(String.format("%s cannot contain negative values (requested: %s).",
                                                   REVIEW_IDS_PARAM, negativeReviewIds));
    }

    return Collections.unmodifiableSet(reviewIds);
  }

  /**
   * Mutually exclusive with the other parameters and can only be used if two step verification is enabled.
   * @param requestContext the request context.
   * @param twoStepVerificationEnabled {@code true} if two-step verification is enabled, {@code false} otherwise.
   * @return Review Id.
   */
  public static Integer reviewId(CruiseControlRequestContext requestContext, boolean twoStepVerificationEnabled) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), REVIEW_ID_PARAM);
    if (parameterString == null) {
      return null;
    } else if (!twoStepVerificationEnabled) {
      throw new UserRequestException(
          String.format("%s parameter is not relevant when two-step verification is disabled.", REVIEW_ID_PARAM));
    }

    Integer reviewId = Integer.parseInt(requestContext.getParameter(parameterString));
    // Sanity check: Ensure that if a review id is provided, no other parameter is in the request.
    if (requestContext.getParameterMap().size() != 1) {
      throw new UserRequestException(
          String.format("%s parameter must be mutually exclusive with other parameters (Request parameters: %s).",
                      REVIEW_ID_PARAM, requestContext.getParameterMap()));
    } else if (reviewId < 0) {
      throw new UserRequestException(String.format("%s cannot be negative (requested: %d).", REVIEW_ID_PARAM, reviewId));
    }

    return reviewId;
  }

  /**
   * Get the execution progress check interval in milliseconds. Default: {@code null}.
   *
   * To prevent setting this value to a very small value by mistake, the Executor ensures that the requested execution
   * progress check interval is not smaller than the configured minimum limit
   * (see {@link com.linkedin.kafka.cruisecontrol.executor.Executor#setRequestedExecutionProgressCheckIntervalMs}).
   *
   * @param requestContext The Http request.
   * @return Execution progress check interval in milliseconds.
   */
  static Long executionProgressCheckIntervalMs(CruiseControlRequestContext requestContext) {
    return getLongParam(requestContext, EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM, null);
  }

  /**
   * Get the execution concurrency requirement dynamically set from the Http request for different concurrency types.
   * @param requestContext                        The Http request.
   * @param concurrencyType Concurrency type.
   * @return The execution concurrency requirement dynamically set from the Http request.
   */
  static Integer concurrentMovements(CruiseControlRequestContext requestContext,
                                     ConcurrencyType concurrencyType) {
    String parameter = "";
    switch (concurrencyType) {
      case INTER_BROKER_REPLICA:
        parameter = CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM;
        break;
      case INTRA_BROKER_REPLICA:
        parameter = CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PARAM;
        break;
      case LEADERSHIP_BROKER:
        parameter = BROKER_CONCURRENT_LEADER_MOVEMENTS_PARAM;
        break;
      case LEADERSHIP_CLUSTER:
        parameter = CONCURRENT_LEADER_MOVEMENTS_PARAM;
        break;
      default:
        throw new IllegalArgumentException("Unsupported concurrency type " + concurrencyType + " is provided.");
    }

    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), parameter);
    if (parameterString == null) {
      return null;
    }
    int concurrentMovementsPerBroker = Integer.parseInt(requestContext.getParameter(parameterString));
    if (concurrentMovementsPerBroker <= 0) {
      throw new UserRequestException("The requested movement concurrency must be positive (Requested: " + concurrentMovementsPerBroker + ").");
    }

    return concurrentMovementsPerBroker;
  }

  /**
   * Get the max inter broker partition movements requirement dynamically set from the Http request.
   *
   * @param requestContext The HTTP Request
   * @return The execution upper bound requirement dynamically set from the Http request.
   */
  static Integer maxPartitionMovements(CruiseControlRequestContext requestContext) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), MAX_PARTITION_MOVEMENTS_IN_CLUSTER_PARAM);
    if (parameterString == null) {
      return null;
    }
    int maxPartitionMovements = Integer.parseInt(requestContext.getParameter(parameterString));
    if (maxPartitionMovements <= 0) {
      throw new UserRequestException("The requested max partition movement must be positive (Requested: " + maxPartitionMovements + ").");
    }
    return maxPartitionMovements;
  }

  static Pattern excludedTopics(CruiseControlRequestContext requestContext) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), EXCLUDED_TOPICS_PARAM);
    return parameterString == null ? null : Pattern.compile(requestContext.getParameter(parameterString));
  }

  /**
   * Default: {@link Integer#MAX_VALUE} for upper bound parameter, or {@link Integer#MIN_VALUE} otherwise.
   * @param requestContext Http servlet request.
   * @param isUpperBound {@code true} if upper bound, {@code false} if lower bound.
   * @return The value of {@link #PARTITION_PARAM} parameter.
   */
  static int partitionBoundary(CruiseControlRequestContext requestContext, boolean isUpperBound) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), PARTITION_PARAM);
    if (parameterString == null) {
      return isUpperBound ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    }
    String partitionString = requestContext.getParameter(parameterString);
    if (!partitionString.contains("-")) {
      return Integer.parseInt(partitionString);
    }

    String[] boundaries = partitionString.split("-");
    if (boundaries.length > 2) {
      throw new UserRequestException("The " + PARTITION_PARAM + " parameter cannot contain multiple dashes.");
    }
    return Integer.parseInt(boundaries[isUpperBound ? 1 : 0]);
  }

  /**
   * Get the {@link #NUM_BROKERS_TO_ADD} from the request.
   *
   * Default: {@link ProvisionRecommendation#DEFAULT_OPTIONAL_INT}
   * @param requestContext Http servlet request.
   * @return The value of {@link #NUM_BROKERS_TO_ADD} parameter.
   * @throws UserRequestException if the number of brokers to add is not a positive integer.
   */
  static int numBrokersToAdd(CruiseControlRequestContext requestContext) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), NUM_BROKERS_TO_ADD);
    if (parameterString == null) {
      return ProvisionRecommendation.DEFAULT_OPTIONAL_INT;
    }
    int numBrokersToAdd = Integer.parseInt(requestContext.getParameter(parameterString));
    if (numBrokersToAdd <= 0) {
      throw new UserRequestException("The requested number of brokers to add must be positive (Requested: " + numBrokersToAdd + ").");
    }
    return numBrokersToAdd;
  }

  /**
   * Get the {@link #PARTITION_COUNT} from the request.
   *
   * Default: {@link ProvisionRecommendation#DEFAULT_OPTIONAL_INT}
   * @param requestContext Http servlet request.
   * @return The value of {@link #PARTITION_COUNT} parameter.
   * @throws UserRequestException if the targeted partition count is not a positive integer.
   */
  static int partitionCount(CruiseControlRequestContext requestContext) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), PARTITION_COUNT);
    if (parameterString == null) {
      return ProvisionRecommendation.DEFAULT_OPTIONAL_INT;
    }
    int targetPartitionCount = Integer.parseInt(requestContext.getParameter(parameterString));
    if (targetPartitionCount <= 0) {
      throw new UserRequestException("The requested targeted partition count must be positive (Requested: " + targetPartitionCount + ").");
    }
    return targetPartitionCount;
  }

  static Set<Integer> brokerIds(CruiseControlRequestContext requestContext, boolean isOptional) throws UnsupportedEncodingException {
    Set<Integer> brokerIds = parseParamToIntegerSet(requestContext, BROKER_ID_PARAM);
    if (!isOptional && brokerIds.isEmpty()) {
      EndPoint endpoint = endPoint(requestContext);
      if (endpoint == DEMOTE_BROKER) {
        // If it is a demote_broker request, either target broker or target disk should be specified in request.
        if (brokerIdAndLogdirs(requestContext).isEmpty()) {
          throw new UserRequestException("No target broker ID or target disk logdir is specified to demote.");
        }
      } else if (endpoint != FIX_OFFLINE_REPLICAS) {
        throw new UserRequestException(String.format("Target broker ID is not provided for %s request", endpoint));
      }
    }
    return Collections.unmodifiableSet(brokerIds);
  }

  static Set<Integer> dropRecentlyRemovedBrokers(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    return Collections.unmodifiableSet(parseParamToIntegerSet(requestContext, DROP_RECENTLY_REMOVED_BROKERS_PARAM));
  }

  static Set<Integer> dropRecentlyDemotedBrokers(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    return Collections.unmodifiableSet(parseParamToIntegerSet(requestContext, DROP_RECENTLY_DEMOTED_BROKERS_PARAM));
  }

  /**
   * Default: An empty set.
   * @param requestContext the request handler.
   * @return The value of {@link #DESTINATION_BROKER_IDS_PARAM} parameter.
   */
  static Set<Integer> destinationBrokerIds(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    Set<Integer> brokerIds = Collections.unmodifiableSet(parseParamToIntegerSet(requestContext, DESTINATION_BROKER_IDS_PARAM));
    if (!brokerIds.isEmpty()) {
      if (isKafkaAssignerMode(requestContext)) {
        throw new UserRequestException("Kafka assigner mode does not support explicitly specifying destination broker ids.");
      }

      // Sanity check: Ensure that BROKER_ID_PARAM and DESTINATION_BROKER_IDS_PARAM configs share nothing.
      ensureDisjoint(parseParamToIntegerSet(requestContext, BROKER_ID_PARAM), brokerIds,
                     "No overlap is allowed between the specified destination broker ids and broker ids");
    }

    return brokerIds;
  }

  /**
   * Default: An empty set.
   * @param requestContext the request context.
   * @param isApprove {@code true} for {@link #APPROVE_PARAM}, {@code false} for {@link #DISCARD_PARAM}
   * @return The value of {@link #APPROVE_PARAM} parameter for approve or {@link #DISCARD_PARAM} parameter for discard.
   */
  private static Set<Integer> review(CruiseControlRequestContext requestContext, boolean isApprove) throws UnsupportedEncodingException {
    Set<Integer> parsedReview = parseParamToIntegerSet(requestContext, isApprove ? APPROVE_PARAM : DISCARD_PARAM);
    return Collections.unmodifiableSet(parsedReview);
  }

  /**
   * Get {@link ReviewStatus#APPROVED} and {@link ReviewStatus#DISCARDED} requests via {@link #APPROVE_PARAM} and
   * {@link #DISCARD_PARAM}.
   *
   * Sanity check ensures that the same request cannot be specified in both configs.
   * @param requestContext the request context.
   * @return {@link ReviewStatus#APPROVED} and {@link ReviewStatus#DISCARDED} requests via {@link #APPROVE_PARAM} and {@link #DISCARD_PARAM}.
   */
  static Map<ReviewStatus, Set<Integer>> reviewRequests(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    Set<Integer> approve = review(requestContext, true);
    Set<Integer> discard = review(requestContext, false);

    // Sanity check: Ensure that the same request is not specified in both configs at the same request.
    ensureDisjoint(approve, discard, "The same request cannot be specified in both approve and discard parameters");
    // Sanity check: Ensure that at least one approve or discard parameter is specified.
    if (approve.isEmpty() && discard.isEmpty()) {
      throw new UserRequestException(String.format("%s endpoint requires at least one of '%s' or '%s' parameter.",
                                                   REVIEW, APPROVE_PARAM, DISCARD_PARAM));
    }

    Map<ReviewStatus, Set<Integer>> reviewRequest = Map.of(APPROVED, approve, DISCARDED, discard);
    return reviewRequest;
  }

  /**
   * Default: An empty map.
   * @param requestContext the request context.
   * @return The value for {@link #BROKER_ID_AND_LOGDIRS_PARAM} parameter.
   */
  static Map<Integer, Set<String>> brokerIdAndLogdirs(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    final Map<Integer, Set<String>> brokerIdAndLogdirs = new HashMap<>();
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), BROKER_ID_AND_LOGDIRS_PARAM);
    if (parameterString != null) {
      Arrays.stream(urlDecode(requestContext.getParameter(parameterString)).split(",")).forEach(e -> {
        int index = e.indexOf(DELIMITER_BETWEEN_BROKER_ID_AND_LOGDIR);
        int brokerId = Integer.parseInt(e.substring(0, index));
        brokerIdAndLogdirs.putIfAbsent(brokerId, new HashSet<>());
        brokerIdAndLogdirs.get(brokerId).add(e.substring(index + 1));
      });
    }
    return Collections.unmodifiableMap(brokerIdAndLogdirs);
  }

  /**
   * Default: An empty set.
   * @param requestContext Http request.
   * @return User task ids.
   */
  public static Set<UUID> userTaskIds(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), USER_TASK_IDS_PARAM);
    return parameterString == null
           ? Collections.emptySet()
            : Arrays.stream(urlDecode(requestContext.getParameter(parameterString)).split(",")).map(UUID::fromString).collect(Collectors.toSet());
  }

  /**
   * Default: An empty set.
   * @param requestContext Http request.
   * @return Client ids.
   */
  public static Set<String> clientIds(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    Set<String> parsedClientIds = parseParamToStringSet(requestContext, CLIENT_IDS_PARAM);
    // May need to validate clientIds
    return Collections.unmodifiableSet(parsedClientIds);
  }

  /**
   * Default: An empty set.
   * @param requestContext Http request.
   * @return Endpoints.
   */
  public static Set<CruiseControlEndPoint> endPoints(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    Set<String> parsedEndPoints = parseParamToStringSet(requestContext, ENDPOINTS_PARAM).stream()
                                                                                 .map(String::toUpperCase)
                                                                                 .collect(Collectors.toSet());

    Set<CruiseControlEndPoint> endPoints = new HashSet<>();
    for (CruiseControlEndPoint endPoint : CruiseControlEndPoint.cachedValues()) {
      if (parsedEndPoints.contains(endPoint.toString())) {
        endPoints.add(endPoint);
      }
    }
    return Collections.unmodifiableSet(endPoints);
  }

  /**
   * Default: An empty set.
   * @param requestContext Http request.
   * @return Types.
   */
  public static Set<UserTaskManager.TaskState> types(CruiseControlRequestContext requestContext) throws UnsupportedEncodingException {
    Set<String> parsedTaskStates = parseParamToStringSet(requestContext, TYPES_PARAM);

    Set<UserTaskManager.TaskState> taskStates = new HashSet<>();
    for (UserTaskManager.TaskState state : UserTaskManager.TaskState.cachedValues()) {
      if (parsedTaskStates.contains(state.toString())) {
        taskStates.add(state);
      }
    }
    return Collections.unmodifiableSet(taskStates);
  }

  /**
   * Default: {@link DataFrom#VALID_WINDOWS}
   * @param requestContext Http request.
   * @return The value for {@link #DATA_FROM_PARAM} parameter.
   */
  static DataFrom getDataFrom(CruiseControlRequestContext requestContext) {
    DataFrom dataFrom = DataFrom.VALID_WINDOWS;
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), DATA_FROM_PARAM);
    if (parameterString != null) {
      dataFrom = DataFrom.valueOf(requestContext.getParameter(parameterString).toUpperCase());
    }
    return dataFrom;
  }

  /**
   * Check whether to skip the hard goal check.
   * @param requestContext the request context.
   * @return {@code true} if hard goal check should be skipped, {@code false} otherwise.
   */
  static boolean skipHardGoalCheck(CruiseControlRequestContext requestContext) {
    return isKafkaAssignerMode(requestContext) || isRebalanceDiskMode(requestContext)
            || getBooleanParam(requestContext, SKIP_HARD_GOAL_CHECK_PARAM, false);
  }

  static boolean skipUrpDemotion(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, SKIP_URP_DEMOTION_PARAM, true);
  }

  static boolean excludeFollowerDemotion(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, EXCLUDE_FOLLOWER_DEMOTION_PARAM, true);
  }

  static Short replicationFactor(CruiseControlRequestContext requestContext) {
    String parameterString = caseSensitiveParameterName(requestContext.getParameterMap(), REPLICATION_FACTOR_PARAM);
    if (parameterString == null) {
      return null;
    }
    return Short.parseShort(requestContext.getParameter(parameterString));
  }

  static boolean fetchCompletedTask(CruiseControlRequestContext requestContext) {
    return getBooleanParam(requestContext, FETCH_COMPLETED_TASK_PARAM, false);
  }

  /**
   * Check whether all the passed-in parameters are {@code null} or not.
   *
   * @param parameters Arbitrary number of parameters to check.
   * @return {@code true} if all parameters are null; {@code false} otherwise.
   */
  public static boolean areAllParametersNull(CruiseControlParameters... parameters) {
    return Arrays.stream(parameters).allMatch(Objects::isNull);
  }

  public enum DataFrom {
    VALID_WINDOWS, VALID_PARTITIONS
  }

}
