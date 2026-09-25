/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveDisksParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PauseResumeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.StopProposalParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TrainParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.BootstrapParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserTasksParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReviewBoardParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReviewParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.FixOfflineReplicasParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AdminParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserPermissionsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RightsizeParameters;
import org.apache.kafka.common.config.ConfigDef;


/**
 * A class to keep Cruise Control Parameters Configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public final class CruiseControlParametersConfig {
    public static final String DEFAULT_PAUSE_RESUME_PARAMETERS_CLASS = PauseResumeParameters.class.getName();

    /**
     * <code>stop.proposal.parameters.class</code>
     */
    public static final String STOP_PROPOSAL_PARAMETERS_CLASS_CONFIG = "stop.proposal.parameters.class";
    public static final String DEFAULT_STOP_PROPOSAL_PARAMETERS_CLASS = StopProposalParameters.class.getName();
    public static final String STOP_PROPOSAL_PARAMETERS_CLASS_DOC = "The class for parameters of a stop proposal execution request.";

    /**
     * <code>bootstrap.parameters.class</code>
     */
    public static final String BOOTSTRAP_PARAMETERS_CLASS_CONFIG = "bootstrap.parameters.class";
    public static final String DEFAULT_BOOTSTRAP_PARAMETERS_CLASS = BootstrapParameters.class.getName();
    public static final String BOOTSTRAP_PARAMETERS_CLASS_DOC = "The class for parameters of a bootstrap request.";

    /**
     * <code>train.parameters.class</code>
     */
    public static final String TRAIN_PARAMETERS_CLASS_CONFIG = "train.parameters.class";
    public static final String DEFAULT_TRAIN_PARAMETERS_CLASS = TrainParameters.class.getName();
    public static final String TRAIN_PARAMETERS_CLASS_DOC = "The class for parameters of a train request.";

    /**
     * <code>load.parameters.class</code>
     */
    public static final String LOAD_PARAMETERS_CLASS_CONFIG = "load.parameters.class";
    public static final String DEFAULT_LOAD_PARAMETERS_CLASS = ClusterLoadParameters.class.getName();
    public static final String LOAD_PARAMETERS_CLASS_DOC = "The class for parameters of a load request.";

    /**
     * <code>partition.load.parameters.class</code>
     */
    public static final String PARTITION_LOAD_PARAMETERS_CLASS_CONFIG = "partition.load.parameters.class";
    public static final String DEFAULT_PARTITION_LOAD_PARAMETERS_CLASS = PartitionLoadParameters.class.getName();
    public static final String PARTITION_LOAD_PARAMETERS_CLASS_DOC = "The class for parameters of a partition load request.";

    /**
     * <code>proposals.parameters.class</code>
     */
    public static final String PROPOSALS_PARAMETERS_CLASS_CONFIG = "proposals.parameters.class";
    public static final String DEFAULT_PROPOSALS_PARAMETERS_CLASS = ProposalsParameters.class.getName();
    public static final String PROPOSALS_PARAMETERS_CLASS_DOC = "The class for parameters of a proposals request.";

    /**
     * <code>state.parameters.class</code>
     */
    public static final String STATE_PARAMETERS_CLASS_CONFIG = "state.parameters.class";
    public static final String DEFAULT_STATE_PARAMETERS_CLASS = CruiseControlStateParameters.class.getName();
    public static final String STATE_PARAMETERS_CLASS_DOC = "The class for parameters of a state request.";

    /**
     * <code>kafka.cluster.state.parameters.class</code>
     */
    public static final String KAFKA_CLUSTER_STATE_PARAMETERS_CLASS_CONFIG = "kafka.cluster.state.parameters.class";
    public static final String DEFAULT_KAFKA_CLUSTER_STATE_PARAMETERS_CLASS = KafkaClusterStateParameters.class.getName();
    public static final String KAFKA_CLUSTER_STATE_PARAMETERS_CLASS_DOC = "The class for parameters of a kafka cluster state request.";

    /**
     * <code>user.tasks.parameters.class</code>
     */
    public static final String USER_TASKS_PARAMETERS_CLASS_CONFIG = "user.tasks.parameters.class";
    public static final String DEFAULT_USER_TASKS_PARAMETERS_CLASS = UserTasksParameters.class.getName();
    public static final String USER_TASKS_PARAMETERS_CLASS_DOC = "The class for parameters of a user tasks request.";

    /**
     * <code>review.board.parameters.class</code>
     */
    public static final String REVIEW_BOARD_PARAMETERS_CLASS_CONFIG = "review.board.parameters.class";
    public static final String DEFAULT_REVIEW_BOARD_PARAMETERS_CLASS = ReviewBoardParameters.class.getName();
    public static final String REVIEW_BOARD_PARAMETERS_CLASS_DOC = "The class for parameters of a review board request.";

    /**
     * <code>add.broker.parameters.class</code>
     */
    public static final String ADD_BROKER_PARAMETERS_CLASS_CONFIG = "add.broker.parameters.class";
    public static final String DEFAULT_ADD_BROKER_PARAMETERS_CLASS = AddBrokerParameters.class.getName();
    public static final String ADD_BROKER_PARAMETERS_CLASS_DOC = "The class for parameters of a add broker request.";

    /**
     * <code>remove.broker.parameters.class</code>
     */
    public static final String REMOVE_BROKER_PARAMETERS_CLASS_CONFIG = "remove.broker.parameters.class";
    public static final String DEFAULT_REMOVE_BROKER_PARAMETERS_CLASS = RemoveBrokerParameters.class.getName();
    public static final String REMOVE_BROKER_PARAMETERS_CLASS_DOC = "The class for parameters of a remove broker request.";

    /**
     * <code>fix.offline.replicas.parameters.class</code>
     */
    public static final String FIX_OFFLINE_REPLICAS_PARAMETERS_CLASS_CONFIG = "fix.offline.replicas.parameters.class";
    public static final String DEFAULT_FIX_OFFLINE_REPLICAS_PARAMETERS_CLASS = FixOfflineReplicasParameters.class.getName();
    public static final String FIX_OFFLINE_REPLICAS_PARAMETERS_CLASS_DOC = "The class for parameters of a fix offline replicas request.";

    /**
     * <code>rebalance.parameters.class</code>
     */
    public static final String REBALANCE_PARAMETERS_CLASS_CONFIG = "rebalance.parameters.class";
    public static final String DEFAULT_REBALANCE_PARAMETERS_CLASS = RebalanceParameters.class.getName();
    public static final String REBALANCE_PARAMETERS_CLASS_DOC = "The class for parameters of a rebalance request.";

    /**
     * <code>pause.sampling.parameters.class</code>
     */
    public static final String PAUSE_SAMPLING_PARAMETERS_CLASS_CONFIG = "pause.sampling.parameters.class";
    public static final String PAUSE_SAMPLING_PARAMETERS_CLASS_DOC = "The class for parameters of a pause sampling request.";

    /**
     * <code>resume.sampling.parameters.class</code>
     */
    public static final String RESUME_SAMPLING_PARAMETERS_CLASS_CONFIG = "resume.sampling.parameters.class";
    public static final String RESUME_SAMPLING_PARAMETERS_CLASS_DOC = "The class for parameters of a resume sampling request.";

    /**
     * <code>demote.broker.parameters.class</code>
     */
    public static final String DEMOTE_BROKER_PARAMETERS_CLASS_CONFIG = "demote.broker.parameters.class";
    public static final String DEFAULT_DEMOTE_BROKER_PARAMETERS_CLASS = DemoteBrokerParameters.class.getName();
    public static final String DEMOTE_BROKER_PARAMETERS_CLASS_DOC = "The class for parameters of a demote broker request.";

    /**
     * <code>admin.parameters.class</code>
     */
    public static final String ADMIN_PARAMETERS_CLASS_CONFIG = "admin.parameters.class";
    public static final String DEFAULT_ADMIN_PARAMETERS_CLASS = AdminParameters.class.getName();
    public static final String ADMIN_PARAMETERS_CLASS_DOC = "The class for parameters of an admin request.";

    /**
     * <code>review.parameters.class</code>
     */
    public static final String REVIEW_PARAMETERS_CLASS_CONFIG = "review.parameters.class";
    public static final String DEFAULT_REVIEW_PARAMETERS_CLASS = ReviewParameters.class.getName();
    public static final String REVIEW_PARAMETERS_CLASS_DOC = "The class for parameters of a review request.";

    /**
     * <code>topic.configuration.parameters.class</code>
     */
    public static final String TOPIC_CONFIGURATION_PARAMETERS_CLASS_CONFIG = "topic.configuration.parameters.class";
    public static final String DEFAULT_TOPIC_CONFIGURATION_PARAMETERS_CLASS = TopicConfigurationParameters.class.getName();
    public static final String TOPIC_CONFIGURATION_PARAMETERS_CLASS_DOC = "The class for parameters of a topic configuration request.";

    /**
     * <code>rightsize.parameters.class</code>
     */
    public static final String RIGHTSIZE_PARAMETERS_CLASS_CONFIG = "rightsize.parameters.class";
    public static final String DEFAULT_RIGHTSIZE_PARAMETERS_CLASS = RightsizeParameters.class.getName();
    public static final String RIGHTSIZE_PARAMETERS_CLASS_DOC = "The class for parameters of a provision rightsize request.";

    /**
     * <code>permissions.parameters.class</code>
     */
    public static final String PERMISSIONS_PARAMETERS_CLASS_CONFIG = "permissions.parameters.class";
    public static final String DEFAULT_PERMISSIONS_PARAMETERS_CLASS = UserPermissionsParameters.class.getName();
    public static final String PERMISSIONS_PARAMETERS_CLASS_DOC = "The class for parameters of user permissons request.";

    /**
     * <code>remove.disks.parameters.class</code>
     */
    public static final String REMOVE_DISKS_PARAMETERS_CLASS_CONFIG = "remove.disks.parameters.class";
    public static final String DEFAULT_REMOVE_DISKS_PARAMETERS_CLASS = RemoveDisksParameters.class.getName();
    public static final String REMOVE_DISKS_PARAMETERS_CLASS_DOC = "The class for parameters of a disks removal request.";

    private CruiseControlParametersConfig() {
    }

    /**
     * Define configs for Cruise Control Parameters.
     *
     * @param configDef Config definition.
     * @return The given ConfigDef after defining the configs for Cruise Control Parameters.
     */
    public static ConfigDef define(ConfigDef configDef) {
        return configDef.define(STOP_PROPOSAL_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_STOP_PROPOSAL_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        STOP_PROPOSAL_PARAMETERS_CLASS_DOC)
                .define(BOOTSTRAP_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_BOOTSTRAP_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        BOOTSTRAP_PARAMETERS_CLASS_DOC)
                .define(TRAIN_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_TRAIN_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        TRAIN_PARAMETERS_CLASS_DOC)
                .define(LOAD_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_LOAD_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        LOAD_PARAMETERS_CLASS_DOC)
                .define(PARTITION_LOAD_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_PARTITION_LOAD_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        PARTITION_LOAD_PARAMETERS_CLASS_DOC)
                .define(PROPOSALS_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_PROPOSALS_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        PROPOSALS_PARAMETERS_CLASS_DOC)
                .define(STATE_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_STATE_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        STATE_PARAMETERS_CLASS_DOC)
                .define(KAFKA_CLUSTER_STATE_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_KAFKA_CLUSTER_STATE_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        KAFKA_CLUSTER_STATE_PARAMETERS_CLASS_DOC)
                .define(USER_TASKS_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_USER_TASKS_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        USER_TASKS_PARAMETERS_CLASS_DOC)
                .define(REVIEW_BOARD_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_REVIEW_BOARD_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        REVIEW_BOARD_PARAMETERS_CLASS_DOC)
                .define(ADD_BROKER_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_ADD_BROKER_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        ADD_BROKER_PARAMETERS_CLASS_DOC)
                .define(REMOVE_BROKER_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_REMOVE_BROKER_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        REMOVE_BROKER_PARAMETERS_CLASS_DOC)
                .define(FIX_OFFLINE_REPLICAS_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_FIX_OFFLINE_REPLICAS_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        FIX_OFFLINE_REPLICAS_PARAMETERS_CLASS_DOC)
                .define(REBALANCE_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_REBALANCE_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        REBALANCE_PARAMETERS_CLASS_DOC)
                .define(PAUSE_SAMPLING_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_PAUSE_RESUME_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        PAUSE_SAMPLING_PARAMETERS_CLASS_DOC)
                .define(RESUME_SAMPLING_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_PAUSE_RESUME_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        RESUME_SAMPLING_PARAMETERS_CLASS_DOC)
                .define(DEMOTE_BROKER_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_DEMOTE_BROKER_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        DEMOTE_BROKER_PARAMETERS_CLASS_DOC)
                .define(ADMIN_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_ADMIN_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        ADMIN_PARAMETERS_CLASS_DOC)
                .define(REVIEW_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_REVIEW_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        REVIEW_PARAMETERS_CLASS_DOC)
                .define(TOPIC_CONFIGURATION_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_TOPIC_CONFIGURATION_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        TOPIC_CONFIGURATION_PARAMETERS_CLASS_DOC)
                .define(RIGHTSIZE_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_RIGHTSIZE_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        RIGHTSIZE_PARAMETERS_CLASS_DOC)
                .define(PERMISSIONS_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_PERMISSIONS_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        PERMISSIONS_PARAMETERS_CLASS_DOC)
                .define(REMOVE_DISKS_PARAMETERS_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_REMOVE_DISKS_PARAMETERS_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        REMOVE_DISKS_PARAMETERS_CLASS_DOC);
    }
}
