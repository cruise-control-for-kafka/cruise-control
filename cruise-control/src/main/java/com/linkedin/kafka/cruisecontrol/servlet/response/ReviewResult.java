/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.RequestInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;


@JsonResponseClass
public class ReviewResult extends AbstractCruiseControlResponse {
  @JsonResponseField
  protected static final String REQUEST_INFO = "RequestInfo";
  protected final Map<Integer, RequestInfo> _requestInfoById;
  protected final Set<Integer> _filteredRequestIds;

  /**
   * @param requestInfoById Request info by Id.
   * @param filteredRequestIds Requests for which the result is requested, empty set implies all requests in review board
   */
  public ReviewResult(Map<Integer, RequestInfo> requestInfoById,
                      Set<Integer> filteredRequestIds,
                      KafkaCruiseControlConfig config) {
    super(config);
    _requestInfoById = requestInfoById;
    _filteredRequestIds = filteredRequestIds.isEmpty() ? _requestInfoById.keySet() : filteredRequestIds;
  }

  protected String getPlaintext() {
    StringBuilder sb = new StringBuilder();
    int padding = 2;
    // Plaintext response, each column name will be underscore-separated instead of case-separated.
    int idLabelSize = RequestInfo.ID.length();
    int submitterAddressLabelSize = RequestInfo.SUBMITTER_ADDRESS.length() + 1;
    // Returns submission_time -- i.e. not submission_time_ms.
    int submissionTimeLabelSize = RequestInfo.SUBMISSION_TIME_MS.length() - 2;
    int statusLabelSize = RequestInfo.STATUS.length();
    int endpointWithParamsLabelSize = RequestInfo.ENDPOINT_WITH_PARAMS.length() + 2;
    int reasonLabelSize = RequestInfo.REASON.length();

    for (Map.Entry<Integer, RequestInfo> entry : _requestInfoById.entrySet()) {
      if (_filteredRequestIds.contains(entry.getKey())) {
        // ID
        idLabelSize = Math.max(idLabelSize, (int) (Math.log10(entry.getKey()) + 1));
        RequestInfo requestInfo = entry.getValue();
        // SUBMITTER_ADDRESS
        submitterAddressLabelSize = Math.max(submitterAddressLabelSize, requestInfo.submitterAddress().length());
        // SUBMISSION_TIME_MS
        String dateFormatted = utcDateFor(requestInfo.submissionTimeMs());
        submissionTimeLabelSize = Math.max(submissionTimeLabelSize, dateFormatted.length());
        // STATUS
        statusLabelSize = Math.max(statusLabelSize, requestInfo.status().toString().length());
        // ENDPOINT_WITH_PARAMS
        endpointWithParamsLabelSize = Math.max(endpointWithParamsLabelSize, requestInfo.endpointWithParams().length());
        // REASON
        reasonLabelSize = Math.max(reasonLabelSize, requestInfo.reason().length());
      }
    }

    // Populate header.
    StringBuilder formattingStringBuilder = new StringBuilder("%n%-");
    formattingStringBuilder.append(idLabelSize + padding)
                           .append("s%-")
                           .append(submitterAddressLabelSize + padding)
                           .append("s%-")
                           .append(submissionTimeLabelSize + padding)
                           .append("s%-")
                           .append(statusLabelSize + padding)
                           .append("s%-")
                           .append(endpointWithParamsLabelSize + padding)
                           .append("s%-")
                           .append(reasonLabelSize + padding)
                           .append("s");
    sb.append(String.format(formattingStringBuilder.toString(), "ID", "SUBMITTER_ADDRESS", "SUBMISSION_TIME", "STATUS",
                            "ENDPOINT_WITH_PARAMS", "REASON"));

    // Populate values.
    for (Map.Entry<Integer, RequestInfo> entry : _requestInfoById.entrySet()) {
      if (_filteredRequestIds.contains(entry.getKey())) {
        RequestInfo requestInfo = entry.getValue();
        String dateFormatted = utcDateFor(requestInfo.submissionTimeMs());
        sb.append(String.format(formattingStringBuilder.toString(), entry.getKey(), requestInfo.submitterAddress(),
                                dateFormatted, requestInfo.status(), requestInfo.endpointWithParams(), requestInfo.reason()));
      }
    }

    return sb.toString();
  }

  protected String getJsonString() {
    List<Map<String, Object>> jsonRequestInfoList = new ArrayList<>(_filteredRequestIds.size());
    for (Map.Entry<Integer, RequestInfo> entry : _requestInfoById.entrySet()) {
      if (_filteredRequestIds.contains(entry.getKey())) {
        jsonRequestInfoList.add(entry.getValue().getJsonStructure(entry.getKey()));
      }
    }
    Map<String, Object> jsonResponse = Map.of(REQUEST_INFO, jsonRequestInfoList, VERSION, JSON_VERSION);
    return new Gson().toJson(jsonResponse);
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJsonString() : getPlaintext();
    // Discard irrelevant response.
    _requestInfoById.clear();
    _filteredRequestIds.clear();
  }
}
