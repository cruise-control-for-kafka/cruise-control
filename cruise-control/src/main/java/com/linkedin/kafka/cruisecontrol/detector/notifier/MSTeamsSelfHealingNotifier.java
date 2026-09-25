/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;

public class MSTeamsSelfHealingNotifier extends SelfHealingNotifier {

    public static final String MSTEAMS_SELF_HEALING_NOTIFIER_WEBHOOK = "msteams.self.healing.notifier.webhook";
    private static final Logger LOG = LoggerFactory.getLogger(MSTeamsSelfHealingNotifier.class);
    protected String _msTeamsWebhook;

    public MSTeamsSelfHealingNotifier() {
    }

    public MSTeamsSelfHealingNotifier(Time time) {
        super(time);
    }

    @Override
    public void configure(Map<String, ?> config) {
        super.configure(config);
        _msTeamsWebhook = (String) config.get(MSTEAMS_SELF_HEALING_NOTIFIER_WEBHOOK);
    }

    @Override
    public void alert(Anomaly anomaly, boolean autoFixTriggered, long selfHealingStartTime, AnomalyType anomalyType) {
        super.alert(anomaly, autoFixTriggered, selfHealingStartTime, anomalyType);

        if (_msTeamsWebhook == null) {
            LOG.warn("MSTeams webhook is null, can't send MSTeams self healing notification");
            return;
        }

        Map<String, String> facts = new HashMap<>(Map.of("Anomaly type", anomalyType.toString(),
                "Anomaly", anomaly.toString(),
                "Self Healing enabled", Boolean.toString(_selfHealingEnabled.get(anomalyType)),
                "Auto fix triggered", Boolean.toString(autoFixTriggered)));
        if (_selfHealingEnabled.get(anomalyType)) {
            facts.put("Self Healing start time", utcDateFor(selfHealingStartTime));
        }

        try {
            sendMSTeamsMessage(new MSTeamsMessage(facts), _msTeamsWebhook);
        } catch (IOException e) {
            LOG.warn("ERROR sending alert to MSTeams", e);
        }
    }

    protected void sendMSTeamsMessage(MSTeamsMessage slackMessage, String msTeamsWebhookUrl) throws IOException {
        NotifierUtils.sendMessage(slackMessage.toString(), msTeamsWebhookUrl, null);
    }
}
