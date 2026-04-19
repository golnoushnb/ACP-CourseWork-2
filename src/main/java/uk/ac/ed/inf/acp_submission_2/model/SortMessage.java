package uk.ac.ed.inf.acp_submission_2.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SortMessage {

    @JsonProperty("Id")
    private int id;

    @JsonProperty("Payload")
    private String payload;

    public SortMessage() {
    }

    public SortMessage(int id, String payload) {
        this.id = id;
        this.payload = payload;
    }

    @JsonProperty("Id")
    public int getId() {
        return id;
    }

    @JsonProperty("Id")
    public void setId(int id) {
        this.id = id;
    }

    @JsonProperty("Payload")
    public String getPayload() {
        return payload;
    }

    @JsonProperty("Payload")
    public void setPayload(String payload) {
        this.payload = payload;
    }
}