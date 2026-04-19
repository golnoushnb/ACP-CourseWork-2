package uk.ac.ed.inf.acp_submission_2.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SplitterMessage {

    @JsonProperty("Id")
    private int id;

    @JsonProperty("Value")
    private double value;

    @JsonProperty("AdditionalData")
    private String additionalData;

    public SplitterMessage() {
    }

    public SplitterMessage(int id, double value, String additionalData) {
        this.id = id;
        this.value = value;
        this.additionalData = additionalData;
    }

    @JsonProperty("Id")
    public int getId() {
        return id;
    }

    @JsonProperty("Id")
    public void setId(int id) {
        this.id = id;
    }

    @JsonProperty("Value")
    public double getValue() {
        return value;
    }

    @JsonProperty("Value")
    public void setValue(double value) {
        this.value = value;
    }

    @JsonProperty("AdditionalData")
    public String getAdditionalData() {
        return additionalData;
    }

    @JsonProperty("AdditionalData")
    public void setAdditionalData(String additionalData) {
        this.additionalData = additionalData;
    }
}