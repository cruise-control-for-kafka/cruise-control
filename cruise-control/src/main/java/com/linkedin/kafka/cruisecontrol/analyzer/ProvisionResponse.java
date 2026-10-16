/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


/**
 * Indicates the {@link ProvisionStatus} along with the recommended actions regarding the relevant status.
 * Recommendations are only relevant to {@link ProvisionStatus#UNDER_PROVISIONED} and {@link ProvisionStatus#OVER_PROVISIONED}.
 */
public class ProvisionResponse {
  public static final String DEFAULT_RECOMMENDATION = "N/A";
  private ProvisionStatus _status;
  private final StringBuilder _humanReadableRecommendation;
  private final Map<String, ProvisionRecommendation> _recommendationByRecommender;

  /**
   * Constructor to be used for provision statuses, for which the recommendations are relevant.
   * Recommender is expected to be a human-readable string.
   *
   * @param status The current provision status.
   * @param provisionRecommendation Recommended action regarding the given provision status.
   * @param recommender The source of the recommended action to be used in aggregate recommendation.
   */
  public ProvisionResponse(ProvisionStatus status, ProvisionRecommendation provisionRecommendation, String recommender) {
    this(status);
    if (!(status == ProvisionStatus.UNDER_PROVISIONED || status == ProvisionStatus.OVER_PROVISIONED)) {
      throw new IllegalArgumentException(String.format("Recommendation is irrelevant for provision status %s.", status));
    }
    validateNotNull(recommender, "The recommender cannot be null.");
    if (provisionRecommendation == null) {
      // The recommendation can be null if the recommender has no recommendation.
      _humanReadableRecommendation.append(String.format("[%s] %s", recommender, DEFAULT_RECOMMENDATION));

    } else {
      _humanReadableRecommendation.append(String.format("[%s] %s", recommender, provisionRecommendation));
      _recommendationByRecommender.put(recommender, provisionRecommendation);
    }
  }

  /**
   * Constructor to be used for provision statuses, for which the recommendations are irrelevant.
   */
  public ProvisionResponse(ProvisionStatus status) {
    validateNotNull(status, "The provision status cannot be null.");
    _status = status;
    _humanReadableRecommendation = new StringBuilder();
    _recommendationByRecommender = new HashMap<>();
  }

  /**
   * @return The current provision status.
   */
  public ProvisionStatus status() {
    return _status;
  }

  /**
   * @return Human-readable recommended actions regarding the current provision status along with the recommender of each action.
   * If there is no recommended action from a specific recommender, the action will default to {@link #DEFAULT_RECOMMENDATION}.
   */
  public String recommendation() {
    return _humanReadableRecommendation.toString();
  }

  /**
   * @return Provision recommendation by the recommender in a programmatically readable format.
   */
  public Map<String, ProvisionRecommendation> recommendationByRecommender() {
    return Collections.unmodifiableMap(_recommendationByRecommender);
  }

  /**
   * Aggregate the given provision response to this provision response using the following rules: Aggregating ...
   * <ul>
   *   <li>any provision status with {@link ProvisionStatus#UNDER_PROVISIONED} is {@link ProvisionStatus#UNDER_PROVISIONED}.</li>
   *   <li>a provision status {@code P} with {@link ProvisionStatus#UNDECIDED} is {@code P}.</li>
   *   <li>{@link ProvisionStatus#RIGHT_SIZED} with {@link ProvisionStatus#RIGHT_SIZED} or {@link ProvisionStatus#OVER_PROVISIONED}
   *   is {@link ProvisionStatus#RIGHT_SIZED}.</li>
   *   <li>{@link ProvisionStatus#OVER_PROVISIONED} with {@link ProvisionStatus#OVER_PROVISIONED} yields itself.</li>
   * </ul>
   *
   * Note that these rules enforce that once a state changes from {@link ProvisionStatus#OVER_PROVISIONED} to another state, it cannot go
   * back to this state. Similarly, once a state goes into {@link ProvisionStatus#UNDER_PROVISIONED}, no other followup state is possible.
   * Hence, {@link #_humanReadableRecommendation} for over- or under-provisioned status can be updated without losing relevant information.
   *
   * @param other Provision response to aggregate into this response.
   * @return This provision response after the aggregation.
   */
  public ProvisionResponse aggregate(ProvisionResponse other) {
    if (_status == ProvisionStatus.UNDER_PROVISIONED) {
      if (other.status() == ProvisionStatus.UNDER_PROVISIONED) {
        aggregateRecommendations(other);
      }
    } else {
      switch (other.status()) {
        case UNDER_PROVISIONED:
          _status = ProvisionStatus.UNDER_PROVISIONED;
          clearRecommendation();
          aggregateRecommendations(other);
          break;
        case RIGHT_SIZED:
          _status = ProvisionStatus.RIGHT_SIZED;
          clearRecommendation();
          break;
        case OVER_PROVISIONED:
          if (_status == ProvisionStatus.OVER_PROVISIONED || _status == ProvisionStatus.UNDECIDED) {
            _status = ProvisionStatus.OVER_PROVISIONED;
            aggregateRecommendations(other);
            break;
          }
          // Keep the status as right-sized if it was right-sized before.
          break;
        case UNDECIDED:
          // Nothing to do.
          break;
        default:
          throw new IllegalArgumentException("Unsupported provision status " + other + " is provided.");
      }
    }
    return this;
  }

  private void clearRecommendation() {
    if (!_humanReadableRecommendation.isEmpty()) {
      _humanReadableRecommendation.setLength(0);
    }
    _recommendationByRecommender.clear();
  }

  private void aggregateRecommendations(ProvisionResponse other) {
    String otherRecommendation = other.recommendation();
    if (!otherRecommendation.isEmpty()) {
      _humanReadableRecommendation.append(_humanReadableRecommendation.isEmpty() ? "" : " ").append(otherRecommendation);
    }
    _recommendationByRecommender.putAll(other.recommendationByRecommender());
  }

  @Override
  public String toString() {
    return String.format("%s%s", _status, _humanReadableRecommendation.isEmpty() ? "" : String.format(" (%s)", recommendation()));
  }
}
