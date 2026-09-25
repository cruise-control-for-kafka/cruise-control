/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;

public class SlackSelfHealingNotifier extends SelfHealingNotifier {

    private static final Logger LOG = LoggerFactory.getLogger(SlackSelfHealingNotifier.class);
    public static final String SLACK_SELF_HEALING_NOTIFIER_WEBHOOK = "slack.self.healing.notifier.webhook";
    public static final String SLACK_SELF_HEALING_NOTIFIER_ICON = "slack.self.healing.notifier.icon";
    public static final String SLACK_SELF_HEALING_NOTIFIER_USER = "slack.self.healing.notifier.user";
    public static final String SLACK_SELF_HEALING_NOTIFIER_CHANNEL = "slack.self.healing.notifier.channel";
    public static final String SLACK_SELF_HEALING_NOTIFIER_PREAMBLE = "slack.self.healing.notifier.preamble";

    public static final String DEFAULT_SLACK_SELF_HEALING_NOTIFIER_ICON = ":information_source:";
    public static final String DEFAULT_SLACK_SELF_HEALING_NOTIFIER_USER = "Cruise Control";
    public static final String DEFAULT_SLACK_SELF_HEALING_NOTIFIER_PREAMBLE = "";

    protected String _slackWebhook;
    protected String _slackIcon;
    protected String _slackChannel;
    protected String _slackUser;
    protected String _slackPreamble;

    public SlackSelfHealingNotifier() {
    }

    public SlackSelfHealingNotifier(Time time) {
        super(time);
    }

    @Override
    public void configure(Map<String, ?> config) {
        super.configure(config);
        _slackWebhook = (String) config.get(SLACK_SELF_HEALING_NOTIFIER_WEBHOOK);
        _slackIcon = (String) config.get(SLACK_SELF_HEALING_NOTIFIER_ICON);
        _slackChannel = (String) config.get(SLACK_SELF_HEALING_NOTIFIER_CHANNEL);
        _slackUser = (String) config.get(SLACK_SELF_HEALING_NOTIFIER_USER);
        _slackPreamble = (String) config.get(SLACK_SELF_HEALING_NOTIFIER_PREAMBLE);
        _slackIcon = _slackIcon == null ? DEFAULT_SLACK_SELF_HEALING_NOTIFIER_ICON : _slackIcon;
        _slackUser = _slackUser == null ? DEFAULT_SLACK_SELF_HEALING_NOTIFIER_USER : _slackUser;
        _slackPreamble = _slackPreamble == null ? DEFAULT_SLACK_SELF_HEALING_NOTIFIER_PREAMBLE : _slackPreamble;
    }

    @Override
    public void alert(Anomaly anomaly, boolean autoFixTriggered, long selfHealingStartTime, AnomalyType anomalyType) {
        super.alert(anomaly, autoFixTriggered, selfHealingStartTime, anomalyType);

        if (_slackWebhook == null) {
            LOG.warn("Slack webhook is null, can't send Slack self healing notification");
            return;
        }

        if (_slackChannel == null) {
            LOG.warn("Slack channel name is null, can't send Slack self healing notification");
            return;
        }

        String text = String.format("%s%s detected %s. Self healing %s.%s", _slackPreamble, anomalyType, anomaly,
                _selfHealingEnabled.get(anomalyType) ? String.format("start time %s", utcDateFor(selfHealingStartTime))
                        : "is disabled",
                autoFixTriggered ? "%nSelf-healing has been triggered." : "");

        try {
            sendSlackMessage(new SlackMessage(_slackUser, text, _slackIcon, _slackChannel), _slackWebhook);
        } catch (IOException e) {
            LOG.warn("ERROR sending alert to Slack", e);
        }
    }

    protected void sendSlackMessage(SlackMessage slackMessage, String slackWebhookUrl) throws IOException {
        NotifierUtils.sendMessage(slackMessage.toString(), slackWebhookUrl, null);
    }
}
