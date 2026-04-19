package uk.ac.ed.inf.acp_submission_2.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TombstoneSummary {

    @JsonProperty("totalMessagesWritten")
    private int totalMessagesWritten;

    @JsonProperty("totalMessagesProcessed")
    private int totalMessagesProcessed;

    @JsonProperty("totalRedisUpdates")
    private int totalRedisUpdates;

    @JsonProperty("totalValueWritten")
    private double totalValueWritten;

    @JsonProperty("totalAdded")
    private double totalAdded;

    public TombstoneSummary() {
    }

    public TombstoneSummary(int totalMessagesWritten,
                            int totalMessagesProcessed,
                            int totalRedisUpdates,
                            double totalValueWritten,
                            double totalAdded) {
        this.totalMessagesWritten = totalMessagesWritten;
        this.totalMessagesProcessed = totalMessagesProcessed;
        this.totalRedisUpdates = totalRedisUpdates;
        this.totalValueWritten = totalValueWritten;
        this.totalAdded = totalAdded;
    }

    @JsonProperty("totalMessagesWritten")
    public int getTotalMessagesWritten() {
        return totalMessagesWritten;
    }

    @JsonProperty("totalMessagesWritten")
    public void setTotalMessagesWritten(int totalMessagesWritten) {
        this.totalMessagesWritten = totalMessagesWritten;
    }

    @JsonProperty("totalMessagesProcessed")
    public int getTotalMessagesProcessed() {
        return totalMessagesProcessed;
    }

    @JsonProperty("totalMessagesProcessed")
    public void setTotalMessagesProcessed(int totalMessagesProcessed) {
        this.totalMessagesProcessed = totalMessagesProcessed;
    }

    @JsonProperty("totalRedisUpdates")
    public int getTotalRedisUpdates() {
        return totalRedisUpdates;
    }

    @JsonProperty("totalRedisUpdates")
    public void setTotalRedisUpdates(int totalRedisUpdates) {
        this.totalRedisUpdates = totalRedisUpdates;
    }

    @JsonProperty("totalValueWritten")
    public double getTotalValueWritten() {
        return totalValueWritten;
    }

    @JsonProperty("totalValueWritten")
    public void setTotalValueWritten(double totalValueWritten) {
        this.totalValueWritten = totalValueWritten;
    }

    @JsonProperty("totalAdded")
    public double getTotalAdded() {
        return totalAdded;
    }

    @JsonProperty("totalAdded")
    public void setTotalAdded(double totalAdded) {
        this.totalAdded = totalAdded;
    }
}