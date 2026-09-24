/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.purgatory;

import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReviewResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.REVIEW;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.POST_METHOD;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.httpServletRequestToString;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.hasValidParameterNames;


/**
 * A Class to keep POST requests that are awaiting review if two-step verification is enabled.
 * <p>
 * The Purgatory is thread-safe, and is relevant only when
 * {@link WebServerConfig#TWO_STEP_VERIFICATION_ENABLED_CONFIG} is enabled.
 */
public class Purgatory implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Purgatory.class);
    private static final long PURGATORY_CLEANER_PERIOD_SECONDS = 10;
    private static final long PURGATORY_CLEANER_INITIAL_DELAY_SECONDS = 0;
    private int _requestId;
    private final long _purgatoryRetentionTimeMs;
    private final Map<Integer, RequestInfo> _requestInfoById;
    private final ScheduledExecutorService _purgatoryCleaner =
            Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("PurgatoryCleaner"));
    private final KafkaCruiseControlConfig _config;

    public Purgatory(KafkaCruiseControlConfig config) {
        _requestId = 0;
        _config = config;
        _purgatoryRetentionTimeMs = config.getLong(WebServerConfig.TWO_STEP_PURGATORY_RETENTION_TIME_MS_CONFIG);
        int purgatoryMaxCachedRequests = config.getInt(WebServerConfig.TWO_STEP_PURGATORY_MAX_REQUESTS_CONFIG);

        _requestInfoById = new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, RequestInfo> eldest) {
                return this.size() > purgatoryMaxCachedRequests;
            }
        };

        _purgatoryCleaner.scheduleAtFixedRate(new PurgatoryCleaner(),
                PURGATORY_CLEANER_INITIAL_DELAY_SECONDS,
                PURGATORY_CLEANER_PERIOD_SECONDS,
                TimeUnit.SECONDS);
    }

    /**
     * Add request to the purgatory and return the {@link ReviewResult} for the request that has been added to
     * the purgatory.
     *
     * @param requestContext the request context.
     * @param parameters     Request parameters.
     * @param <P>            Type corresponding to the request parameters.
     * @return The result showing the {@link ReviewResult} for the request that has been added to the purgatory.
     */
    private synchronized <P extends CruiseControlParameters> ReviewResult addRequest(CruiseControlRequestContext requestContext,
                                                                                     P parameters) {
        if (!requestContext.getMethod().equals(POST_METHOD)) {
            throw new IllegalArgumentException(String.format("Purgatory can only contain POST request (Attempted to add: %s).",
                    httpServletRequestToString(requestContext)));
        }
        RequestInfo requestInfo = new RequestInfo(requestContext, parameters);
        _requestInfoById.put(_requestId, requestInfo);

        Map<Integer, RequestInfo> requestInfoById = new HashMap<>();
        requestInfoById.put(_requestId, requestInfo);
        Set<Integer> filteredRequestIds = new HashSet<>();
        filteredRequestIds.add(_requestId);

        ReviewResult result = new ReviewResult(requestInfoById, filteredRequestIds, _config);
        _requestId++;
        return result;
    }

    /**
     * Add the given request to the purgatory unless:
     * <ul>
     *   <li>Request is already in the purgatory and contains the corresponding reviewId to retrieve its parameters.</li>
     *   <li>Request contains invalid parameter names.</li>
     *   <li>Parameters specified in the request cannot be parsed.</li>
     * </ul>
     *
     * @param classConfig              Config indicating the class of the pluggable parameter.
     * @param requestContext           the request context.
     * @param parameterConfigOverrides Configs to override upon creating the pluggable parameter.
     * @param userTaskManager          a reference to {@link UserTaskManager}
     * @return Parameters of the request if it is in the purgatory, and requested with the corresponding reviewId,
     * {@code null} otherwise.
     */
    public CruiseControlParameters maybeAddToPurgatory(CruiseControlRequestContext requestContext,
                                                       String classConfig,
                                                       Map<String, Object> parameterConfigOverrides,
                                                       UserTaskManager userTaskManager) throws IOException {
        Integer reviewId = ParameterUtils.reviewId(requestContext, true);
        if (reviewId != null) {
            // Submit the request with reviewId that should already be in the purgatory associated with the request endpoint.
            RequestInfo requestInfo = submit(reviewId, requestContext);
            // Ensure that if the request has already been submitted, the user is not attempting to create another user task
            // with the same parameters and endpoint.
            sanityCheckSubmittedRequest(requestContext, requestInfo, userTaskManager);

            return requestInfo.parameters();
        } else {
            CruiseControlParameters parameters = _config.getConfiguredInstance(classConfig,
                    CruiseControlParameters.class,
                    parameterConfigOverrides);
            if (hasValidParameterNames(requestContext, parameters) && !parameters.parseParameters(requestContext)) {
                // Add request to purgatory and return ReviewResult.
                ReviewResult reviewResult = addRequest(requestContext, parameters);
                reviewResult.writeSuccessResponse(parameters, requestContext);
                LOG.info("Added request {} (parameters: {}) to purgatory.", requestContext.getPathInfo(), requestContext.getParameterMap());
            }

            return null;
        }
    }

    private static void sanityCheckSubmittedRequest(CruiseControlRequestContext requestContext, RequestInfo requestInfo,
                                                    UserTaskManager userTaskManager) {
        if (requestInfo.accessToAlreadySubmittedRequest()
                && userTaskManager.getUserTaskByUserTaskId(userTaskManager.getUserTaskId(requestContext), requestContext) == null) {
            throw new UserRequestException(
                    String.format("Attempt to start a new user task with an already submitted review. If you are trying to retrieve"
                            + " the result of a submitted execution, please use its UUID in your request header via %s flag."
                            + " If you are starting a new execution with the same parameters, please submit a new review "
                            + "request and get approval for it.", UserTaskManager.USER_TASK_HEADER_NAME));
        }
    }

    /**
     * Ensure that:
     * <ul>
     *   <li>A request with the given review id exists in the purgatory.</li>
     *   <li>The request with the given review id matches the given request.</li>
     *   <li>The request with the given review id is approved in the purgatory.</li>
     * </ul>
     * <p>
     * Then mark the review status as submitted.
     *
     * @param reviewId       The review id for which the corresponding request is requested to be submitted.
     * @param requestContext the request context.
     * @return Submitted request info.
     */
    public synchronized RequestInfo submit(int reviewId, CruiseControlRequestContext requestContext) {
        RequestInfo requestInfo = _requestInfoById.get(reviewId);
        // 1. Ensure that a request with the given review id exists in the purgatory.
        if (requestInfo == null) {
            throw new UserRequestException(
                    String.format("No request with review id %d exists in purgatory. Please use %s endpoint to check for the "
                            + "current requests awaiting review in purgatory.", reviewId, REVIEW));
        }

        // 2. Ensure that the request with the given review id matches the given request.
        CruiseControlEndPoint endpoint = ParameterUtils.endPoint(requestContext);
        if (requestInfo.endPoint() != endpoint) {
            throw new UserRequestException(
                    String.format("Request with review id %d is associated with %s endpoint, but the given request has %s endpoint."
                                    + "Please use %s endpoint to check for the current requests awaiting review in purgatory.",
                            reviewId, requestInfo.endPoint(), endpoint, REVIEW));
        }

        if (requestInfo.status() == ReviewStatus.SUBMITTED) {
            LOG.info("Request {} has already been submitted (review: {}).", requestInfo.endpointWithParams(), reviewId);
            requestInfo.setAccessToAlreadySubmittedRequest();
        } else {
            // 3. Ensure that the request with the given review id is approved in the purgatory, and mark the status as submitted.
            requestInfo.submitReview(reviewId);
            LOG.info("Submitted request {} for execution (review: {}).", requestInfo.endpointWithParams(), reviewId);
        }
        return requestInfo;
    }

    /**
     * Remove the {@link ReviewStatus#SUBMITTED} request associated with the given review id from purgatory.
     *
     * @param reviewId Review id of the request to be removed from the purgatory.
     * @return Removed submitted request if exists in purgatory, null otherwise.
     */
    public synchronized RequestInfo removeSubmitted(int reviewId) {
        RequestInfo requestInfo = _requestInfoById.get(reviewId);
        if (requestInfo == null) {
            return null;
        } else if (requestInfo.status() != ReviewStatus.SUBMITTED) {
            throw new IllegalStateException(
                    String.format("Attempt to remove request associated with review id %d from purgatory. Status (current %s, "
                            + "expected: %s).", reviewId, requestInfo.status(), ReviewStatus.SUBMITTED));
        }

        return _requestInfoById.remove(reviewId);
    }

    /**
     * Get requested reviews from the review board.
     *
     * @param reviewIds Requests for which the result is requested, empty set implies all requests in the review board.
     * @return The requested reviews from the review board.
     */
    public synchronized ReviewResult reviewBoard(Set<Integer> reviewIds) {
        return new ReviewResult(new HashMap<>(_requestInfoById), reviewIds, _config);
    }

    /**
     * Apply the given target states to review the corresponding requests. Get the post-review result of the purgatory.
     *
     * @param requestIdsByTargetState Request Ids by target review state for requests in purgatory.
     * @param reason                  Common reason for applying the review to the requests.
     * @return The result showing the current purgatory state after the review.
     */
    public synchronized ReviewResult applyReview(Map<ReviewStatus, Set<Integer>> requestIdsByTargetState, String reason) {
        // Sanity check if all request ids in the review exists in the purgatory.
        Set<Integer> reviewedRequestIds = new HashSet<>();
        for (Map.Entry<ReviewStatus, Set<Integer>> entry : requestIdsByTargetState.entrySet()) {
            Set<Integer> requestIds = entry.getValue();
            if (!_requestInfoById.keySet().containsAll(requestIds)) {
                throw new IllegalStateException(String.format("Review contains request ids (%s) that do not exist in purgatory.",
                        requestIds.removeAll(_requestInfoById.keySet())));
            }
            // Apply review to each Request Info
            ReviewStatus targetReviewStatus = entry.getKey();
            requestIds.forEach(requestId -> _requestInfoById.get(requestId).applyReview(targetReviewStatus, reason));
            reviewedRequestIds.addAll(requestIds);
        }

        // Return the post-review result of the purgatory.
        return new ReviewResult(new HashMap<>(_requestInfoById), reviewedRequestIds, _config);
    }

    private synchronized void removeOldRequests() {
        LOG.debug("Remove old requests from purgatory.");
        _requestInfoById.entrySet().removeIf(entry -> (entry.getValue().submissionTimeMs()
                + _purgatoryRetentionTimeMs < System.currentTimeMillis()));
    }

    @Override
    public void close() {
        _purgatoryCleaner.shutdownNow();
        _requestInfoById.clear();
    }

    /**
     * A runnable class to remove expired requests.
     */
    private class PurgatoryCleaner implements Runnable {
        @Override
        public void run() {
            try {
                removeOldRequests();
            } catch (Throwable t) {
                LOG.warn("Received exception when trying to remove old requests from purgatory.", t);
            }
        }
    }
}
