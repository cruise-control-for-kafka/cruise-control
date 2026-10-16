/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class ProvisionResponseTest {
  private static final double DELTA = 0.01;
  private static final String RECOMMENDER_UP = "Recommender-Under-Provisioned";
  private static final String RECOMMENDER_OP = "Recommender-Over-Provisioned";

  private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.00", DecimalFormatSymbols.getInstance());

  private static final int NUM_BROKERS_UP = 6;
  private static final int TYPICAL_BROKER_ID_UP = 0;
  private static final double TYPICAL_BROKER_CAPACITY_UP = 3200;
  private static final String UNDER_PROV_REC_STR = "Add at least 6 brokers with the same cpu capacity ("
      + DECIMAL_FORMAT.format(TYPICAL_BROKER_CAPACITY_UP) + ") as broker-0.";
  private static final ProvisionRecommendation UNDER_PROV_REC = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
      .numBrokers(NUM_BROKERS_UP).typicalBrokerCapacity(TYPICAL_BROKER_CAPACITY_UP).typicalBrokerId(TYPICAL_BROKER_ID_UP).resource(Resource.CPU)
      .build();

  private static final int NUM_BROKERS_OP = 4;
  private static final int TYPICAL_BROKER_ID_OP = 1;
  private static final double TYPICAL_BROKER_CAPACITY_OP = 1600;
  private static final String OVER_PROV_REC_STR = "Remove at least 4 brokers with the same cpu capacity ("
      + DECIMAL_FORMAT.format(TYPICAL_BROKER_CAPACITY_OP) + ") as broker-1.";
  private static final ProvisionRecommendation OVER_PROV_REC = new ProvisionRecommendation.Builder(ProvisionStatus.OVER_PROVISIONED)
      .numBrokers(NUM_BROKERS_OP).typicalBrokerCapacity(TYPICAL_BROKER_CAPACITY_OP).typicalBrokerId(TYPICAL_BROKER_ID_OP).resource(Resource.CPU)
      .build();

  private static ProvisionResponse generateProvisionResponse(ProvisionStatus status) {
    return switch (status) {
      case UNDER_PROVISIONED -> new ProvisionResponse(ProvisionStatus.UNDER_PROVISIONED, UNDER_PROV_REC, RECOMMENDER_UP);
      case RIGHT_SIZED -> new ProvisionResponse(ProvisionStatus.RIGHT_SIZED);
      case OVER_PROVISIONED -> new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, OVER_PROV_REC, RECOMMENDER_OP);
      case UNDECIDED -> new ProvisionResponse(ProvisionStatus.UNDECIDED);
    };
  }

  @Test
  public void testNullRecommendation() {
    // Verify validity of input while creating a ProvisionResponse using a null recommendation and a valid recommender (no exception).
    String recommenderUp = "nullRecommendationUp";
    String recommenderOp = "nullRecommendationOp";
    ProvisionResponse nullRecommendationUp = new ProvisionResponse(ProvisionStatus.UNDER_PROVISIONED, null, recommenderUp);
    ProvisionResponse nullRecommendationOp = new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, null, recommenderOp);

    // Verify expected status
    assertEquals(ProvisionStatus.UNDER_PROVISIONED, nullRecommendationUp.status());
    assertEquals(ProvisionStatus.OVER_PROVISIONED, nullRecommendationOp.status());

    // Verify recommendation exists with the default value and expected recommender
    assertEquals(String.format("[%s] %s", recommenderUp, ProvisionResponse.DEFAULT_RECOMMENDATION), nullRecommendationUp.recommendation());
    assertEquals(String.format("[%s] %s", recommenderOp, ProvisionResponse.DEFAULT_RECOMMENDATION), nullRecommendationOp.recommendation());

    // Verify recommendationByRecommender is empty
    assertTrue(nullRecommendationUp.recommendationByRecommender().isEmpty());
    assertTrue(nullRecommendationOp.recommendationByRecommender().isEmpty());

    // Verify aggregating with another empty recommendation with the same state.
    String recommenderUp2 = "nullRecommendationUp2";
    String recommenderOp2 = "nullRecommendationOp2";
    ProvisionResponse nullRecommendationUp2 = new ProvisionResponse(ProvisionStatus.UNDER_PROVISIONED, null, recommenderUp2);
    ProvisionResponse nullRecommendationOp2 = new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, null, recommenderOp2);

    nullRecommendationUp.aggregate(nullRecommendationUp2);
    nullRecommendationOp.aggregate(nullRecommendationOp2);

    assertEquals(String.format("[%s] %s [%s] %s", recommenderUp, ProvisionResponse.DEFAULT_RECOMMENDATION, recommenderUp2,
                               ProvisionResponse.DEFAULT_RECOMMENDATION), nullRecommendationUp.recommendation());
    assertEquals(String.format("[%s] %s [%s] %s", recommenderOp, ProvisionResponse.DEFAULT_RECOMMENDATION, recommenderOp2,
                               ProvisionResponse.DEFAULT_RECOMMENDATION), nullRecommendationOp.recommendation());

    // Verify recommendationByRecommender is still empty
    assertTrue(nullRecommendationUp.recommendationByRecommender().isEmpty());
    assertTrue(nullRecommendationOp.recommendationByRecommender().isEmpty());

    // Verify aggregating with a non-empty recommendation with the same state.
    nullRecommendationUp.aggregate(generateProvisionResponse(ProvisionStatus.UNDER_PROVISIONED));
    nullRecommendationOp.aggregate(generateProvisionResponse(ProvisionStatus.OVER_PROVISIONED));

    assertEquals(String.format("[%s] %s [%s] %s [%s] %s", recommenderUp, ProvisionResponse.DEFAULT_RECOMMENDATION, recommenderUp2,
                               ProvisionResponse.DEFAULT_RECOMMENDATION, RECOMMENDER_UP, UNDER_PROV_REC_STR), nullRecommendationUp.recommendation());
    assertEquals(String.format("[%s] %s [%s] %s [%s] %s", recommenderOp, ProvisionResponse.DEFAULT_RECOMMENDATION, recommenderOp2,
                               ProvisionResponse.DEFAULT_RECOMMENDATION, RECOMMENDER_OP, OVER_PROV_REC_STR), nullRecommendationOp.recommendation());

    // Verify recommendationByRecommender is no longer empty and has the expected value
    assertEquals(1, nullRecommendationUp.recommendationByRecommender().size());
    assertEquals(1, nullRecommendationOp.recommendationByRecommender().size());
    assertEquals(NUM_BROKERS_UP, nullRecommendationUp.recommendationByRecommender().get(RECOMMENDER_UP).numBrokers());
    assertEquals(TYPICAL_BROKER_ID_UP, nullRecommendationUp.recommendationByRecommender().get(RECOMMENDER_UP).typicalBrokerId());
    assertEquals(TYPICAL_BROKER_CAPACITY_UP, nullRecommendationUp.recommendationByRecommender().get(RECOMMENDER_UP).typicalBrokerCapacity(), DELTA);
    assertEquals(NUM_BROKERS_OP, nullRecommendationOp.recommendationByRecommender().get(RECOMMENDER_OP).numBrokers());
    assertEquals(TYPICAL_BROKER_ID_OP, nullRecommendationOp.recommendationByRecommender().get(RECOMMENDER_OP).typicalBrokerId());
    assertEquals(TYPICAL_BROKER_CAPACITY_OP, nullRecommendationOp.recommendationByRecommender().get(RECOMMENDER_OP).typicalBrokerCapacity(), DELTA);

    // Verify string representation
    assertEquals(String.format("%s (%s)", ProvisionStatus.UNDER_PROVISIONED, nullRecommendationUp.recommendation()),
                 nullRecommendationUp.toString());
    assertEquals(String.format("%s (%s)", ProvisionStatus.OVER_PROVISIONED, nullRecommendationOp.recommendation()),
                 nullRecommendationOp.toString());
  }

  @Test
  public void testAggregate() {
    // Verify validity of input while creating a ProvisionResponse using an invalid recommender and recommendation.
    assertThrows(IllegalArgumentException.class, () -> new ProvisionResponse(ProvisionStatus.RIGHT_SIZED, OVER_PROV_REC, RECOMMENDER_UP));
    assertThrows(IllegalArgumentException.class, () -> new ProvisionResponse(ProvisionStatus.UNDECIDED, OVER_PROV_REC, RECOMMENDER_UP));

    // Verify validity of input while creating a ProvisionResponse using a valid recommendation, but an invalid (i.e. null) recommender.
    assertThrows(IllegalArgumentException.class, () -> new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, OVER_PROV_REC, null));
    assertThrows(IllegalArgumentException.class, () -> new ProvisionResponse(ProvisionStatus.UNDER_PROVISIONED, UNDER_PROV_REC, null));

    // Verify validity of aggregation (1) state, (2) recommendation, and (3) provision recommendations.
    // Case-1: Aggregating any provision status with {@link ProvisionStatus#UNDER_PROVISIONED} is {@link ProvisionStatus#UNDER_PROVISIONED}.
    String recommender = "Case1";
    for (ProvisionStatus status : ProvisionStatus.cachedValues()) {
      ProvisionResponse underProvisioned = new ProvisionResponse(ProvisionStatus.UNDER_PROVISIONED, UNDER_PROV_REC, recommender);
      underProvisioned.aggregate(generateProvisionResponse(status));
      assertEquals(ProvisionStatus.UNDER_PROVISIONED, underProvisioned.status());
      assertEquals(status == ProvisionStatus.UNDER_PROVISIONED
                   ? String.format("[%s] %s [%s] %s", recommender, UNDER_PROV_REC_STR, RECOMMENDER_UP, UNDER_PROV_REC_STR)
                   : String.format("[%s] %s", recommender, UNDER_PROV_REC_STR), underProvisioned.recommendation());
      assertEquals(status == ProvisionStatus.UNDER_PROVISIONED ? 2 : 1, underProvisioned.recommendationByRecommender().size());
      assertEquals(NUM_BROKERS_UP, underProvisioned.recommendationByRecommender().get(recommender).numBrokers());
      assertEquals(TYPICAL_BROKER_ID_UP, underProvisioned.recommendationByRecommender().get(recommender).typicalBrokerId());
      assertEquals(TYPICAL_BROKER_CAPACITY_UP, underProvisioned.recommendationByRecommender().get(recommender).typicalBrokerCapacity(), DELTA);
      if (status == ProvisionStatus.UNDER_PROVISIONED) {
        assertEquals(NUM_BROKERS_UP, underProvisioned.recommendationByRecommender().get(RECOMMENDER_UP).numBrokers());
        assertEquals(TYPICAL_BROKER_ID_UP, underProvisioned.recommendationByRecommender().get(RECOMMENDER_UP).typicalBrokerId());
        assertEquals(TYPICAL_BROKER_CAPACITY_UP, underProvisioned.recommendationByRecommender().get(RECOMMENDER_UP).typicalBrokerCapacity(), DELTA);
      }
    }

    // Case-2: Aggregating a provision status {@code P} with {@link ProvisionStatus#UNDECIDED} is {@code P}
    for (ProvisionStatus status : ProvisionStatus.cachedValues()) {
      ProvisionResponse undecided = new ProvisionResponse(ProvisionStatus.UNDECIDED);
      ProvisionResponse other = generateProvisionResponse(status);
      String recommendationBefore = other.recommendation();
      Map<String, ProvisionRecommendation> recommendationByRecommenderBefore = other.recommendationByRecommender();
      undecided.aggregate(other);
      assertEquals(status, undecided.status());
      assertEquals(recommendationBefore, undecided.recommendation());
      assertEquals(recommendationByRecommenderBefore, undecided.recommendationByRecommender());
    }

    // Case-3.1: Aggregating {@link ProvisionStatus#RIGHT_SIZED} with {@link ProvisionStatus#RIGHT_SIZED} or
    // {@link ProvisionStatus#OVER_PROVISIONED} is {@link ProvisionStatus#RIGHT_SIZED}
    ProvisionResponse rightSized = new ProvisionResponse(ProvisionStatus.RIGHT_SIZED);
    rightSized.aggregate(generateProvisionResponse(ProvisionStatus.RIGHT_SIZED));
    assertEquals(ProvisionStatus.RIGHT_SIZED, rightSized.status());
    assertTrue(rightSized.recommendationByRecommender().isEmpty());
    rightSized.aggregate(generateProvisionResponse(ProvisionStatus.OVER_PROVISIONED));
    assertEquals(ProvisionStatus.RIGHT_SIZED, rightSized.status());
    assertTrue(rightSized.recommendation().isEmpty());
    assertTrue(rightSized.recommendationByRecommender().isEmpty());

    // Case-3.2: Aggregating {@link ProvisionStatus#OVER_PROVISIONED} with {@link ProvisionStatus#RIGHT_SIZED} clears the recommendation
    recommender = "Case3.2";
    ProvisionResponse overProvisioned = new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, OVER_PROV_REC, recommender);
    assertFalse(overProvisioned.recommendation().isEmpty());
    assertFalse(overProvisioned.recommendationByRecommender().isEmpty());
    overProvisioned.aggregate(generateProvisionResponse(ProvisionStatus.RIGHT_SIZED));
    assertTrue(overProvisioned.recommendation().isEmpty());
    assertTrue(overProvisioned.recommendationByRecommender().isEmpty());

    // Case-4: Aggregating {@link ProvisionStatus#OVER_PROVISIONED} with {@link ProvisionStatus#OVER_PROVISIONED} yields itself
    recommender = "Case4";
    overProvisioned = new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, OVER_PROV_REC, recommender);
    assertEquals(String.format("[%s] %s", recommender, OVER_PROV_REC_STR), overProvisioned.recommendation());
    assertEquals(1, overProvisioned.recommendationByRecommender().size());
    assertEquals(NUM_BROKERS_OP, overProvisioned.recommendationByRecommender().get(recommender).numBrokers());
    assertEquals(TYPICAL_BROKER_ID_OP, overProvisioned.recommendationByRecommender().get(recommender).typicalBrokerId());
    assertEquals(TYPICAL_BROKER_CAPACITY_OP, overProvisioned.recommendationByRecommender().get(recommender).typicalBrokerCapacity(), DELTA);

    overProvisioned.aggregate(generateProvisionResponse(ProvisionStatus.OVER_PROVISIONED));
    assertEquals(ProvisionStatus.OVER_PROVISIONED, overProvisioned.status());
    assertEquals(String.format("[%s] %s [%s] %s", recommender, OVER_PROV_REC_STR, RECOMMENDER_OP, OVER_PROV_REC_STR),
                 overProvisioned.recommendation());
    assertEquals(2, overProvisioned.recommendationByRecommender().size());
    assertEquals(NUM_BROKERS_OP, overProvisioned.recommendationByRecommender().get(RECOMMENDER_OP).numBrokers());
    assertEquals(TYPICAL_BROKER_ID_OP, overProvisioned.recommendationByRecommender().get(RECOMMENDER_OP).typicalBrokerId());
    assertEquals(TYPICAL_BROKER_CAPACITY_OP, overProvisioned.recommendationByRecommender().get(RECOMMENDER_OP).typicalBrokerCapacity(), DELTA);
  }
}
