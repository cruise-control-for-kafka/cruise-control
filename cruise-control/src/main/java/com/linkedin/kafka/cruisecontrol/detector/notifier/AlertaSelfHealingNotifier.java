/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.cruisecontrol.CruiseControlUtils;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import com.linkedin.kafka.cruisecontrol.detector.DiskFailures;
import com.linkedin.kafka.cruisecontrol.detector.GoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.IntraBrokerGoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent;
import com.linkedin.kafka.cruisecontrol.detector.TopicAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.TopicPartitionSizeAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomaly.TopicReplicationFactorAnomalyEntry;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;

/**
 * This class implements the Alerta notification functionality.
 * @see <a href="https://alerta.io">https://alerta.io</a>
 *
 * <ul>
 * <li>{@link #ALERTA_SELF_HEALING_NOTIFIER_API_URL}: Alerta API url. This is the URL to which will be post the different alerts.</li>
 * <li>{@link #ALERTA_SELF_HEALING_NOTIFIER_API_KEY}: Alerta API key used to authenticate.</li>
 * <li>{@link #ALERTA_SELF_HEALING_NOTIFIER_ENVIRONMENT}: Environment attribute used to namespace the Alerta alert resource.</li>
 * </ul>
 */
public class AlertaSelfHealingNotifier extends SelfHealingNotifier {

  private static final Logger LOG = LoggerFactory.getLogger(AlertaSelfHealingNotifier.class);

  public static final String ALERTA_SELF_HEALING_NOTIFIER_API_URL = "alerta.self.healing.notifier.api.url";
  public static final String ALERTA_SELF_HEALING_NOTIFIER_API_KEY = "alerta.self.healing.notifier.api.key";
  public static final String ALERTA_SELF_HEALING_NOTIFIER_ENVIRONMENT = "alerta.self.healing.notifier.environment";

  public static final String ALERT_MESSAGE_PREFIX_TOPIC_PARTITION_SIZE_ANOMALY = "TOPIC_PARTITION_SIZE_ANOMALY - ";
  public static final String ALERT_MESSAGE_PREFIX_TOPIC_REPLICATION_FACTOR_ANOMALY = "TOPIC_REPLICATION_FACTOR_ANOMALY - ";
  public static final String ALERT_MESSAGE_BROKER = "Broker";
  public static final String ALERT_CRUISE_CONTROL = "cruise-control";
  public static final String ALERT_CRUISE_CONTROL_ALARM = "cruiseControlAlarm";
  public static final String ALERT_ALARM_ID_TAG_KEY = "alarm_id";

  protected String _alertaApiUrl;
  protected String _alertaApiKey;
  protected String _alertaEnvironment;

  public AlertaSelfHealingNotifier() {
  }

  public AlertaSelfHealingNotifier(Time time) {
    super(time);
  }

  @Override
  public void configure(Map<String, ?> config) {
    super.configure(config);
    _alertaApiUrl = (String) config.get(ALERTA_SELF_HEALING_NOTIFIER_API_URL);
    _alertaApiKey = (String) config.get(ALERTA_SELF_HEALING_NOTIFIER_API_KEY);
    _alertaEnvironment = (String) config.get(ALERTA_SELF_HEALING_NOTIFIER_ENVIRONMENT);
  }

  @Override
  public void alert(Anomaly anomaly, boolean autoFixTriggered, long selfHealingStartTime, AnomalyType anomalyType) {
    super.alert(anomaly, autoFixTriggered, selfHealingStartTime, anomalyType);

    if (_alertaApiUrl == null) {
      LOG.warn("Alerta API URL is null, can't send Alerta.io self healing notification");
      return;
    }

    if (_alertaApiKey == null) {
      LOG.warn("Alerta API key is null, can't send Alerta.io self healing notification");
      return;
    }

    String text = String.format("%s detected %s. Self healing %s.%s", anomalyType, anomaly,
                                _selfHealingEnabled.get(anomalyType) ? String.format("start time %s", utcDateFor(selfHealingStartTime))
                                                                     : "is disabled",
                                autoFixTriggered ? "%nSelf-healing has been triggered." : "");

    String tmpLocalHostname;
    try {
      tmpLocalHostname = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      LOG.warn("Unable to get the hostname of the Cruise Control server", e);
      tmpLocalHostname = ALERT_CRUISE_CONTROL;
    }
    final String localHostname = tmpLocalHostname;

    List<AlertaMessage> alertaMessages = new ArrayList<>();

    switch ((KafkaAnomalyType) anomalyType) {
      case GOAL_VIOLATION:
        GoalViolations goalViolations = (GoalViolations) anomaly;
        alertGoalViolation(anomalyType, localHostname, alertaMessages, goalViolations);
        break;
      case INTRA_BROKER_GOAL_VIOLATION:
        IntraBrokerGoalViolations intraBrokerGoalViolations = (IntraBrokerGoalViolations) anomaly;
        alertIntraBrokerGoalViolation(anomalyType, localHostname, alertaMessages, intraBrokerGoalViolations);
        break;
      case BROKER_FAILURE:
        BrokerFailures brokerFailures = (BrokerFailures) anomaly;
        alertBrokerFailure(anomalyType, localHostname, alertaMessages, brokerFailures);
        break;
      case METRIC_ANOMALY:
        KafkaMetricAnomaly metricAnomaly = (KafkaMetricAnomaly) anomaly;
        alertMetricAnomaly(anomalyType, localHostname, alertaMessages, metricAnomaly);
        break;
      case DISK_FAILURE:
        DiskFailures diskFailures = (DiskFailures) anomaly;
        alertDiskFailure(anomalyType, localHostname, alertaMessages, diskFailures);
        break;
      case TOPIC_ANOMALY:
        TopicAnomaly topicAnomaly = (TopicAnomaly) anomaly;
        alertTopicAnomaly(anomalyType, localHostname, alertaMessages, topicAnomaly);
        break;
      case MAINTENANCE_EVENT:
        MaintenanceEvent maintenanceEvent = (MaintenanceEvent) anomaly;
        alertMaintenanceEvent(anomalyType, localHostname, alertaMessages, maintenanceEvent);
        break;
      default:
        throw new IllegalStateException("Unrecognized anomaly type.");
    }

    for (AlertaMessage alertaMessage : alertaMessages) {
      alertaMessage.setEnvironment(_alertaEnvironment);
      alertaMessage.setService(Collections.singletonList(ALERT_CRUISE_CONTROL));
      alertaMessage.setText(text);
      alertaMessage.setOrigin(ALERT_CRUISE_CONTROL + "/" + localHostname);
      alertaMessage.setType(ALERT_CRUISE_CONTROL_ALARM);
      alertaMessage.setRawData(anomaly.toString());
      alertaMessage.setTags(Collections.singletonList(ALERT_ALARM_ID_TAG_KEY + ":" + anomaly.anomalyId()));
      try {
        sendAlertaMessage(alertaMessage);
      } catch (IOException e) {
        LOG.warn("ERROR sending alert to Alerta.io", e);
      }
    }
  }

  private void alertMaintenanceEvent(AnomalyType anomalyType, final String localHostname,
                                     List<AlertaMessage> alertaMessages, MaintenanceEvent maintenanceEvent) {
    AlertaMessage alertaMessage = new AlertaMessage(localHostname, anomalyType.toString() + " - " + maintenanceEvent.reasonSupplier());
    alertaMessage.setSeverity(NotifierUtils.getAlertSeverity(anomalyType).toString());
    alertaMessage.setGroup(AlertaAlertGroup.PERFORMANCE.toString());
    alertaMessage.setCreateTime(CruiseControlUtils.utcDateFor(maintenanceEvent.detectionTimeMs(), 3, ChronoUnit.SECONDS));
    alertaMessages.add(alertaMessage);
  }

  private void alertTopicAnomaly(AnomalyType anomalyType, final String localHostname,
                                 List<AlertaMessage> alertaMessages, TopicAnomaly topicAnomaly) {
    if (topicAnomaly instanceof TopicPartitionSizeAnomaly) {
      TopicPartitionSizeAnomaly topicPartitionSizeAnomaly = (TopicPartitionSizeAnomaly) topicAnomaly;
      for (Map.Entry<TopicPartition, Double> entry : topicPartitionSizeAnomaly.sizeInMbByPartition().entrySet()) {
        AlertaMessage alertaMessage = new AlertaMessage(localHostname,
                                                        ALERT_MESSAGE_PREFIX_TOPIC_PARTITION_SIZE_ANOMALY + entry.getKey().toString());
        alertaMessage.setSeverity(NotifierUtils.getAlertSeverity(anomalyType).toString());
        alertaMessage.setGroup(AlertaAlertGroup.PERFORMANCE.toString());
        alertaMessage.setValue(String.format("%f MB", entry.getValue()));
        alertaMessage.setCreateTime(CruiseControlUtils.utcDateFor(topicAnomaly.detectionTimeMs(), 3, ChronoUnit.SECONDS));
        alertaMessages.add(alertaMessage);
      }
    } else if (topicAnomaly instanceof TopicReplicationFactorAnomaly) {
      TopicReplicationFactorAnomaly topicReplicationFactorAnomaly = (TopicReplicationFactorAnomaly) topicAnomaly;
      for (Entry<Short, Set<TopicReplicationFactorAnomalyEntry>> entry : topicReplicationFactorAnomaly.badTopicsByDesiredRF().entrySet()) {
        entry.getValue().forEach(topicReplicationFactorAnomalyEntry -> {
          AlertaMessage alertaMessage = new AlertaMessage(localHostname, ALERT_MESSAGE_PREFIX_TOPIC_REPLICATION_FACTOR_ANOMALY
                                                                         + topicReplicationFactorAnomalyEntry.topicName());
          alertaMessage.setSeverity(NotifierUtils.getAlertSeverity(anomalyType).toString());
          alertaMessage.setGroup(AlertaAlertGroup.PERFORMANCE.toString());
          alertaMessage.setValue(String.format("%.2f", topicReplicationFactorAnomalyEntry.violationRatio()));
          alertaMessage.setCreateTime(CruiseControlUtils.utcDateFor(topicAnomaly.detectionTimeMs(), 3, ChronoUnit.SECONDS));
          alertaMessages.add(alertaMessage);
        });
      }
    } else {
      AlertaMessage alertaMessage = new AlertaMessage(localHostname,
                                                      anomalyType.toString());
      alertaMessage.setSeverity(NotifierUtils.getAlertSeverity(anomalyType).toString());
      alertaMessage.setGroup(AlertaAlertGroup.PERFORMANCE.toString());
      alertaMessage.setCreateTime(CruiseControlUtils.utcDateFor(topicAnomaly.detectionTimeMs(), 3, ChronoUnit.SECONDS));
      alertaMessages.add(alertaMessage);
    }
  }

  private void alertDiskFailure(AnomalyType anomalyType, final String localHostname,
                                List<AlertaMessage> alertaMessages, DiskFailures diskFailures) {
    Map<Integer, Map<String, Long>> failedDisks = diskFailures.failedDisks();
    failedDisks.forEach((brokerId, failures) -> failures.forEach((logdir, eventTime) -> {
      AlertaMessage alertaMessage = new AlertaMessage(localHostname + " - " + ALERT_MESSAGE_BROKER + brokerId,
                                                      anomalyType.toString() + " - " + logdir);
      alertaMessage.setSeverity(NotifierUtils.getAlertSeverity(anomalyType).toString());
      alertaMessage.setGroup(AlertaAlertGroup.STORAGE.toString());
      alertaMessage.setCreateTime(CruiseControlUtils.utcDateFor(eventTime, 3, ChronoUnit.SECONDS));
      alertaMessage.setValue(logdir);
      alertaMessages.add(alertaMessage);
    }));
  }

  private void alertMetricAnomaly(AnomalyType anomalyType, final String localHostname,
                                  List<AlertaMessage> alertaMessages, KafkaMetricAnomaly metricAnomaly) {
    Map<BrokerEntity, Long> brokersEntitiesValues = metricAnomaly.entities();
    for (Entry<BrokerEntity, Long> entry : brokersEntitiesValues.entrySet()) {
      AlertaMessage alertaMessage = new AlertaMessage(localHostname + " - Broker " + entry.getKey().brokerId(),
                                                      anomalyType.toString());
      alertaMessage.setSeverity(NotifierUtils.getAlertSeverity(anomalyType).toString());
      alertaMessage.setGroup(AlertaAlertGroup.PERFORMANCE.toString());
      alertaMessage.setCreateTime(CruiseControlUtils.utcDateFor(entry.getValue(), 3, ChronoUnit.SECONDS));
      alertaMessages.add(alertaMessage);
    }
  }

  private void alertBrokerFailure(AnomalyType anomalyType, final String localHostname,
                                  List<AlertaMessage> alertaMessages, BrokerFailures brokerFailures) {
    Map<Integer, Long> failedBrokers = brokerFailures.failedBrokers();
    for (Entry<Integer, Long> entry : failedBrokers.entrySet()) {
      AlertaMessage alertaMessage = new AlertaMessage(localHostname + " - Broker " + entry.getKey(),
                                                      anomalyType.toString());
      alertaMessage.setSeverity(NotifierUtils.getAlertSeverity(anomalyType).toString());
      alertaMessage.setGroup(AlertaAlertGroup.PERFORMANCE.toString());
      alertaMessage.setCreateTime(CruiseControlUtils.utcDateFor(entry.getValue(), 3, ChronoUnit.SECONDS));
      alertaMessages.add(alertaMessage);
    }
  }

  private void alertGoalViolation(AnomalyType anomalyType,
                                  final String localHostname,
                                  List<AlertaMessage> alertaMessages,
                                  GoalViolations goalViolations) {
      alertGoalViolationLogic(anomalyType,
                              localHostname,
                              alertaMessages,
                              goalViolations.violatedGoalsByFixability(),
                              goalViolations.detectionTimeMs());
    }

    private void alertIntraBrokerGoalViolation(AnomalyType anomalyType,
                                               final String localHostname,
                                               List<AlertaMessage> alertaMessages,
                                               IntraBrokerGoalViolations goalViolations) {
      alertGoalViolationLogic(anomalyType,
                              localHostname,
                              alertaMessages,
                              goalViolations.violatedGoalsByFixability(),
                              goalViolations.detectionTimeMs());
    }

    private void alertGoalViolationLogic(AnomalyType anomalyType,
                                         final String localHostname,
                                         List<AlertaMessage> alertaMessages,
                                         Map<Boolean, List<String>> violations,
                                         long detectionTime) {
    for (Entry<Boolean, List<String>> entry : violations.entrySet()) {
      entry.getValue().forEach(goal -> {
        AlertaMessage alertaMessage = new AlertaMessage(localHostname, anomalyType.toString() + " - " + goal);
        alertaMessage.setSeverity(NotifierUtils.getAlertSeverity(anomalyType).toString());
        alertaMessage.setGroup(AlertaAlertGroup.PERFORMANCE.toString());
        alertaMessage.setCreateTime(CruiseControlUtils.utcDateFor(detectionTime, 3, ChronoUnit.SECONDS));
        // alertaMessage.setCreateTime(CruiseControlUtils.utcDateFor(goalViolations.detectionTimeMs(), 3, ChronoUnit.SECONDS));
        alertaMessages.add(alertaMessage);
      });
    }
  }

  protected void sendAlertaMessage(AlertaMessage alertaMessage) throws IOException {
    NotifierUtils.sendMessage(alertaMessage.toString(), _alertaApiUrl + "/alert", _alertaApiKey);
  }
}
