/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;


/**
 * A serializer / deserializer of {@link MaintenancePlan}.
 */
public class MaintenancePlanSerde implements Serializer<MaintenancePlan>, Deserializer<MaintenancePlan> {
    private final Gson
            _gson = new GsonBuilder().registerTypeAdapter(MaintenancePlan.class, new MaintenancePlanTypeAdapter()).create();

    @Override
    public MaintenancePlan deserialize(String topic, byte[] bytes) {
        try {
            return _gson.fromJson(new String(bytes, StandardCharsets.UTF_8), MaintenancePlan.class);
        } catch (Exception e) {
            throw new RuntimeException("Error occurred while deserializing Maintenance plan.", e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, MaintenancePlan recordValue) {
        return _gson.toJson(recordValue, new TypeToken<MaintenancePlan>() {
        }.getType()).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {

    }

    /**
     * An adapter to support runtime type inference of maintenance plan.
     */
    public static class MaintenancePlanTypeAdapter implements JsonSerializer<MaintenancePlan>, JsonDeserializer<MaintenancePlan> {
        public static final String PLAN_TYPE = "planType";
        public static final String VERSION = "version";
        /* crc of the content */
        public static final String CRC = "crc";
        public static final String CONTENT = "content";

        /**
         * Verifies the crc of the given plan with the crc computed with the given plan.
         *
         * @param maintenancePlan Maintenance plan
         * @param storedCrc       Expected crc of the maintenance plan retrieved via deserialization.
         */
        private static void verifyCrc(MaintenancePlan maintenancePlan, long storedCrc) {
            long computedCrc = maintenancePlan.getCrc();
            if (storedCrc != computedCrc) {
                throw new IllegalArgumentException(String.format("Plan is corrupt. CRC (stored: %d, computed: %d)", storedCrc, computedCrc));
            }
        }

        /**
         * Check whether the maintenance plan with the given type exists and supports the given version
         *
         * @param type    Plan type to verify
         * @param version Version to verify
         */
        private static void verifyTypeAndVersion(String type, byte version) throws UnknownVersionException {
            byte latestSupportedVersion;
            if (AddBrokerPlan.class.getSimpleName().equals(type)) {
                latestSupportedVersion = AddBrokerPlan.LATEST_SUPPORTED_VERSION;
            } else if (RemoveBrokerPlan.class.getSimpleName().equals(type)) {
                latestSupportedVersion = RemoveBrokerPlan.LATEST_SUPPORTED_VERSION;
            } else if (FixOfflineReplicasPlan.class.getSimpleName().equals(type)) {
                latestSupportedVersion = FixOfflineReplicasPlan.LATEST_SUPPORTED_VERSION;
            } else if (RebalancePlan.class.getSimpleName().equals(type)) {
                latestSupportedVersion = RebalancePlan.LATEST_SUPPORTED_VERSION;
            } else if (DemoteBrokerPlan.class.getSimpleName().equals(type)) {
                latestSupportedVersion = DemoteBrokerPlan.LATEST_SUPPORTED_VERSION;
            } else if (TopicReplicationFactorPlan.class.getSimpleName().equals(type)) {
                latestSupportedVersion = TopicReplicationFactorPlan.LATEST_SUPPORTED_VERSION;
            } else {
                // This could happen when a new type of maintenance event is added, but we are still running the old code.
                throw new IllegalArgumentException(String.format("Unsupported plan type: %s", type));
            }

            if (version > latestSupportedVersion) {
                throw new UnknownVersionException(String.format("Cannot deserialize the plan with type %s and version %d. Latest"
                        + " supported version: %d.", type, version, latestSupportedVersion));
            }
        }

        @Override
        public JsonElement serialize(MaintenancePlan maintenancePlan, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject result = new JsonObject();
            result.add(PLAN_TYPE, new JsonPrimitive(maintenancePlan.getClass().getSimpleName()));
            result.add(VERSION, new JsonPrimitive(maintenancePlan.planVersion()));
            result.add(CRC, new JsonPrimitive(maintenancePlan.getCrc()));
            result.add(CONTENT, context.serialize(maintenancePlan, maintenancePlan.getClass()));
            return result;
        }

        @Override
        public MaintenancePlan deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            String type = jsonObject.get(PLAN_TYPE).getAsString();
            byte version = jsonObject.get(VERSION).getAsByte();
            try {
                verifyTypeAndVersion(type, version);
            } catch (UnknownVersionException e) {
                return null;
            }
            long storedCrc = jsonObject.get(CRC).getAsLong();
            JsonElement element = jsonObject.get(CONTENT);

            try {
                String fullName = typeOfT.getTypeName();
                String packageText = fullName.substring(0, fullName.lastIndexOf(".") + 1);

                MaintenancePlan maintenancePlan = context.deserialize(element, Class.forName(packageText + type));
                verifyCrc(maintenancePlan, storedCrc);
                return maintenancePlan;
            } catch (ClassNotFoundException cnfe) {
                throw new JsonParseException("Unknown element type: " + type, cnfe);
            }
        }
    }
}
