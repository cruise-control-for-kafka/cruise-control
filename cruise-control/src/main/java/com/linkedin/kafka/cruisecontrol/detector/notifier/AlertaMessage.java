/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Alerta.io data to create alert: <a href="https://docs.alerta.io/en/latest/api/reference.html#create-an-alert">...</a>
 */
public final class AlertaMessage implements Serializable {
    @Serial
    private static final long serialVersionUID = -7290861136323903837L;

    @NotNull
    @JsonProperty("resource")
    private String _resource;
    @NotNull
    @JsonProperty("event")
    private String _event;

    @JsonProperty("environment")
    private String _environment;
    @JsonProperty("severity")
    private String _severity;
    @JsonProperty("correlate")
    private List<String> _correlate;
    @JsonProperty("status")
    private List<String> _status;
    @JsonProperty("service")
    private List<String> _service;
    @JsonProperty("group")
    private String _group;
    @JsonProperty("value")
    private String _value;
    @JsonProperty("text")
    private String _text;
    @JsonProperty("tags")
    private List<String> _tags;
    @JsonProperty("attributes")
    private Map<String, String> _attributes;
    @JsonProperty("origin")
    private String _origin;
    @JsonProperty("type")
    private String _type;
    @JsonProperty("createTime")
    private String _createTime;
    @JsonProperty("timeout")
    private String _timeout;
    @JsonProperty("rawData")
    private String _rawData;

    public AlertaMessage(String resource, String event) {
        this._event = event;
        this._resource = resource;
    }

    public String getResource() {
        return _resource;
    }

    public void setResource(String resource) {
        this._resource = resource;
    }

    public String getEvent() {
        return _event;
    }

    public void setEvent(String event) {
        this._event = event;
    }

    public String getEnvironment() {
        return _environment;
    }

    public void setEnvironment(String environment) {
        this._environment = environment;
    }

    public String getSeverity() {
        return _severity;
    }

    public void setSeverity(String severity) {
        this._severity = severity;
    }

    public List<String> getCorrelate() {
        return _correlate;
    }

    public void setCorrelate(List<String> correlate) {
        this._correlate = correlate;
    }

    public List<String> getService() {
        return _service;
    }

    public void setService(List<String> service) {
        this._service = service;
    }

    public String getGroup() {
        return _group;
    }

    public void setGroup(String group) {
        this._group = group;
    }

    public String getValue() {
        return _value;
    }

    public void setValue(String value) {
        this._value = value;
    }

    public String getText() {
        return _text;
    }

    public void setText(String text) {
        this._text = text;
    }

    public List<String> getTags() {
        return _tags;
    }

    public void setTags(List<String> tags) {
        this._tags = tags;
    }

    public String getOrigin() {
        return _origin;
    }

    public void setOrigin(String origin) {
        this._origin = origin;
    }

    public String getType() {
        return _type;
    }

    public void setType(String type) {
        this._type = type;
    }

    public String getCreateTime() {
        return _createTime;
    }

    public void setCreateTime(String createTime) {
        this._createTime = createTime;
    }

    public String getTimeout() {
        return _timeout;
    }

    public void setTimeout(String timeout) {
        this._timeout = timeout;
    }

    public String getRawData() {
        return _rawData;
    }

    public void setRawData(String rawData) {
        this._rawData = rawData;
    }

    public List<String> getStatus() {
        return _status;
    }

    public void setStatus(List<String> status) {
        this._status = status;
    }

    public Map<String, String> getAttributes() {
        return _attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this._attributes = attributes;
    }

    @Override
    public String toString() {
        try {
            return new ObjectMapper().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
                .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                .setSerializationInclusion(Include.NON_NULL).writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "AlertaMessage Object parsing error : " + e.getMessage();
        }
    }
}
