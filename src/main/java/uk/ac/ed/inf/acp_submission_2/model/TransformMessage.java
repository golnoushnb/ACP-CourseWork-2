package uk.ac.ed.inf.acp_submission_2.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TransformMessage {

    @JsonProperty("key")
    private String key;

    @JsonProperty("version")
    private Integer version;

    @JsonProperty("value")
    private Double value;

    public TransformMessage() {
    }

    public TransformMessage(String key, Integer version, Double value) {
        this.key = key;
        this.version = version;
        this.value = value;
    }

    @JsonProperty("key")
    public String getKey() {
        return key;
    }

    @JsonProperty("key")
    public void setKey(String key) {
        this.key = key;
    }

    @JsonProperty("version")
    public Integer getVersion() {
        return version;
    }

    @JsonProperty("version")
    public void setVersion(Integer version) {
        this.version = version;
    }

    @JsonProperty("value")
    public Double getValue() {
        return value;
    }

    @JsonProperty("value")
    public void setValue(Double value) {
        this.value = value;
    }
}