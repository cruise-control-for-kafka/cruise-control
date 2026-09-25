/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.ClusterLoadRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.PartitionLoadRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.ProposalsRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.CruiseControlStateRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.TopicConfigurationRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.AddBrokerRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.RemoveBrokerRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.UserPermissionRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.RemoveDisksRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.DemoteRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.RebalanceRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.FixOfflineReplicasRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.RightsizeRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.AdminRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.BootstrapRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.KafkaClusterStateRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.PauseRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.ResumeRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.ReviewBoardRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.ReviewRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.StopProposalRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.TrainRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.UserTasksRequest;
import org.apache.kafka.common.config.ConfigDef;


/**
 * A class to keep Cruise Control Request Configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public final class CruiseControlRequestConfig {

    /**
     * <code>stop.proposal.request.class</code>
     */
    public static final String STOP_PROPOSAL_REQUEST_CLASS_CONFIG = "stop.proposal.request.class";
    public static final String DEFAULT_STOP_PROPOSAL_REQUEST_CLASS = StopProposalRequest.class.getName();
    public static final String STOP_PROPOSAL_REQUEST_CLASS_DOC = "The class to handle a stop proposal execution request.";

    /**
     * <code>bootstrap.request.class</code>
     */
    public static final String BOOTSTRAP_REQUEST_CLASS_CONFIG = "bootstrap.request.class";
    public static final String DEFAULT_BOOTSTRAP_REQUEST_CLASS = BootstrapRequest.class.getName();
    public static final String BOOTSTRAP_REQUEST_CLASS_DOC = "The class to handle a bootstrap request.";

    /**
     * <code>train.request.class</code>
     */
    public static final String TRAIN_REQUEST_CLASS_CONFIG = "train.request.class";
    public static final String DEFAULT_TRAIN_REQUEST_CLASS = TrainRequest.class.getName();
    public static final String TRAIN_REQUEST_CLASS_DOC = "The class to handle a train request.";

    /**
     * <code>load.request.class</code>
     */
    public static final String LOAD_REQUEST_CLASS_CONFIG = "load.request.class";
    public static final String DEFAULT_LOAD_REQUEST_CLASS = ClusterLoadRequest.class.getName();
    public static final String LOAD_REQUEST_CLASS_DOC = "The class to handle a load request.";

    /**
     * <code>partition.load.request.class</code>
     */
    public static final String PARTITION_LOAD_REQUEST_CLASS_CONFIG = "partition.load.request.class";
    public static final String DEFAULT_PARTITION_LOAD_REQUEST_CLASS = PartitionLoadRequest.class.getName();
    public static final String PARTITION_LOAD_REQUEST_CLASS_DOC = "The class to handle a partition load request.";

    /**
     * <code>proposals.request.class</code>
     */
    public static final String PROPOSALS_REQUEST_CLASS_CONFIG = "proposals.request.class";
    public static final String DEFAULT_PROPOSALS_REQUEST_CLASS = ProposalsRequest.class.getName();
    public static final String PROPOSALS_REQUEST_CLASS_DOC = "The class to handle a proposals request.";

    /**
     * <code>state.request.class</code>
     */
    public static final String STATE_REQUEST_CLASS_CONFIG = "state.request.class";
    public static final String DEFAULT_STATE_REQUEST_CLASS = CruiseControlStateRequest.class.getName();
    public static final String STATE_REQUEST_CLASS_DOC = "The class to handle a state request.";

    /**
     * <code>kafka.cluster.state.request.class</code>
     */
    public static final String KAFKA_CLUSTER_STATE_REQUEST_CLASS_CONFIG = "kafka.cluster.state.request.class";
    public static final String DEFAULT_KAFKA_CLUSTER_STATE_REQUEST_CLASS = KafkaClusterStateRequest.class.getName();
    public static final String KAFKA_CLUSTER_STATE_REQUEST_CLASS_DOC = "The class to handle a kafka cluster state request.";

    /**
     * <code>user.tasks.request.class</code>
     */
    public static final String USER_TASKS_REQUEST_CLASS_CONFIG = "user.tasks.request.class";
    public static final String DEFAULT_USER_TASKS_REQUEST_CLASS = UserTasksRequest.class.getName();
    public static final String USER_TASKS_REQUEST_CLASS_DOC = "The class to handle a user tasks request.";

    /**
     * <code>review.board.request.class</code>
     */
    public static final String REVIEW_BOARD_REQUEST_CLASS_CONFIG = "review.board.request.class";
    public static final String DEFAULT_REVIEW_BOARD_REQUEST_CLASS = ReviewBoardRequest.class.getName();
    public static final String REVIEW_BOARD_REQUEST_CLASS_DOC = "The class to handle a review board request.";

    /**
     * <code>add.broker.request.class</code>
     */
    public static final String ADD_BROKER_REQUEST_CLASS_CONFIG = "add.broker.request.class";
    public static final String DEFAULT_ADD_BROKER_REQUEST_CLASS = AddBrokerRequest.class.getName();
    public static final String ADD_BROKER_REQUEST_CLASS_DOC = "The class to handle a add broker request.";

    /**
     * <code>remove.broker.request.class</code>
     */
    public static final String REMOVE_BROKER_REQUEST_CLASS_CONFIG = "remove.broker.request.class";
    public static final String DEFAULT_REMOVE_BROKER_REQUEST_CLASS = RemoveBrokerRequest.class.getName();
    public static final String REMOVE_BROKER_REQUEST_CLASS_DOC = "The class to handle a remove broker request.";

    /**
     * <code>fix.offline.replicas.request.class</code>
     */
    public static final String FIX_OFFLINE_REPLICAS_REQUEST_CLASS_CONFIG = "fix.offline.replicas.request.class";
    public static final String DEFAULT_FIX_OFFLINE_REPLICAS_REQUEST_CLASS = FixOfflineReplicasRequest.class.getName();
    public static final String FIX_OFFLINE_REPLICAS_REQUEST_CLASS_DOC = "The class to handle a fix offline replicas request.";

    /**
     * <code>rebalance.request.class</code>
     */
    public static final String REBALANCE_REQUEST_CLASS_CONFIG = "rebalance.request.class";
    public static final String DEFAULT_REBALANCE_REQUEST_CLASS = RebalanceRequest.class.getName();
    public static final String REBALANCE_REQUEST_CLASS_DOC = "The class to handle a rebalance request.";

    /**
     * <code>pause.sampling.request.class</code>
     */
    public static final String PAUSE_SAMPLING_REQUEST_CLASS_CONFIG = "pause.sampling.request.class";
    public static final String DEFAULT_PAUSE_SAMPLING_REQUEST_CLASS = PauseRequest.class.getName();
    public static final String PAUSE_SAMPLING_REQUEST_CLASS_DOC = "The class to handle a pause sampling request.";

    /**
     * <code>resume.sampling.request.class</code>
     */
    public static final String RESUME_SAMPLING_REQUEST_CLASS_CONFIG = "resume.sampling.request.class";
    public static final String DEFAULT_RESUME_SAMPLING_REQUEST_CLASS = ResumeRequest.class.getName();
    public static final String RESUME_SAMPLING_REQUEST_CLASS_DOC = "The class to handle a resume sampling request.";

    /**
     * <code>demote.broker.request.class</code>
     */
    public static final String DEMOTE_BROKER_REQUEST_CLASS_CONFIG = "demote.broker.request.class";
    public static final String DEFAULT_DEMOTE_BROKER_REQUEST_CLASS = DemoteRequest.class.getName();
    public static final String DEMOTE_BROKER_REQUEST_CLASS_DOC = "The class to handle a demote broker request.";

    /**
     * <code>admin.request.class</code>
     */
    public static final String ADMIN_REQUEST_CLASS_CONFIG = "admin.request.class";
    public static final String DEFAULT_ADMIN_REQUEST_CLASS = AdminRequest.class.getName();
    public static final String ADMIN_REQUEST_CLASS_DOC = "The class to handle an admin request.";

    /**
     * <code>review.request.class</code>
     */
    public static final String REVIEW_REQUEST_CLASS_CONFIG = "review.request.class";
    public static final String DEFAULT_REVIEW_REQUEST_CLASS = ReviewRequest.class.getName();
    public static final String REVIEW_REQUEST_CLASS_DOC = "The class to handle a review request.";

    /**
     * <code>topic.configuration.request.class</code>
     */
    public static final String TOPIC_CONFIGURATION_REQUEST_CLASS_CONFIG = "topic.configuration.request.class";
    public static final String DEFAULT_TOPIC_CONFIGURATION_REQUEST_CLASS = TopicConfigurationRequest.class.getName();
    public static final String TOPIC_CONFIGURATION_REQUEST_CLASS_DOC = "The class to handle a topic configuration request.";

    /**
     * <code>rightsize.request.class</code>
     */
    public static final String RIGHTSIZE_REQUEST_CLASS_CONFIG = "rightsize.request.class";
    public static final String DEFAULT_RIGHTSIZE_REQUEST_CLASS = RightsizeRequest.class.getName();
    public static final String RIGHTSIZE_REQUEST_CLASS_DOC = "The class to handle a provision rightsize request.";

    /**
     * <code>permissions.request.class</code>
     */
    public static final String PERMISSIONS_REQUEST_CLASS_CONFIG = "permissions.request.class";
    public static final String DEFAULT_PERMISSIONS_REQUEST_CLASS = UserPermissionRequest.class.getName();
    public static final String PERMISSIONS_REQUEST_CLASS_DOC = "The class to handle a user permission request.";

    /**
     * <code>remove.disks.request.class</code>
     */
    public static final String REMOVE_DISKS_REQUEST_CLASS_CONFIG = "remove.disks.request.class";
    public static final String DEFAULT_REMOVE_DISKS_REQUEST_CLASS = RemoveDisksRequest.class.getName();
    public static final String REMOVE_DISKS_REQUEST_CLASS_DOC = "The class to handle a disks removal request.";

    private CruiseControlRequestConfig() {
    }

    /**
     * Define configs for Cruise Control Request.
     *
     * @param configDef Config definition.
     * @return The given ConfigDef after defining the configs for Cruise Control Request.
     */
    public static ConfigDef define(ConfigDef configDef) {
        return configDef.define(STOP_PROPOSAL_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_STOP_PROPOSAL_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        STOP_PROPOSAL_REQUEST_CLASS_DOC)
                .define(BOOTSTRAP_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_BOOTSTRAP_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        BOOTSTRAP_REQUEST_CLASS_DOC)
                .define(TRAIN_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_TRAIN_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        TRAIN_REQUEST_CLASS_DOC)
                .define(LOAD_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_LOAD_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        LOAD_REQUEST_CLASS_DOC)
                .define(PARTITION_LOAD_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_PARTITION_LOAD_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        PARTITION_LOAD_REQUEST_CLASS_DOC)
                .define(PROPOSALS_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_PROPOSALS_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        PROPOSALS_REQUEST_CLASS_DOC)
                .define(STATE_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_STATE_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        STATE_REQUEST_CLASS_DOC)
                .define(KAFKA_CLUSTER_STATE_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_KAFKA_CLUSTER_STATE_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        KAFKA_CLUSTER_STATE_REQUEST_CLASS_DOC)
                .define(USER_TASKS_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_USER_TASKS_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        USER_TASKS_REQUEST_CLASS_DOC)
                .define(REVIEW_BOARD_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_REVIEW_BOARD_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        REVIEW_BOARD_REQUEST_CLASS_DOC)
                .define(ADD_BROKER_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_ADD_BROKER_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        ADD_BROKER_REQUEST_CLASS_DOC)
                .define(REMOVE_BROKER_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_REMOVE_BROKER_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        REMOVE_BROKER_REQUEST_CLASS_DOC)
                .define(FIX_OFFLINE_REPLICAS_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_FIX_OFFLINE_REPLICAS_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        FIX_OFFLINE_REPLICAS_REQUEST_CLASS_DOC)
                .define(REBALANCE_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_REBALANCE_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        REBALANCE_REQUEST_CLASS_DOC)
                .define(PAUSE_SAMPLING_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_PAUSE_SAMPLING_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        PAUSE_SAMPLING_REQUEST_CLASS_DOC)
                .define(RESUME_SAMPLING_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_RESUME_SAMPLING_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        RESUME_SAMPLING_REQUEST_CLASS_DOC)
                .define(DEMOTE_BROKER_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_DEMOTE_BROKER_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        DEMOTE_BROKER_REQUEST_CLASS_DOC)
                .define(ADMIN_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_ADMIN_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        ADMIN_REQUEST_CLASS_DOC)
                .define(REVIEW_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_REVIEW_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        REVIEW_REQUEST_CLASS_DOC)
                .define(TOPIC_CONFIGURATION_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_TOPIC_CONFIGURATION_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        TOPIC_CONFIGURATION_REQUEST_CLASS_DOC)
                .define(RIGHTSIZE_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_RIGHTSIZE_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        RIGHTSIZE_REQUEST_CLASS_DOC)
                .define(PERMISSIONS_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_PERMISSIONS_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        PERMISSIONS_REQUEST_CLASS_DOC)
                .define(REMOVE_DISKS_REQUEST_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_REMOVE_DISKS_REQUEST_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        REMOVE_DISKS_REQUEST_CLASS_DOC);
    }
}
