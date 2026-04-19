package uk.ac.ed.inf.acp_submission_2.model;

public class TransformRequest {

    private String readQueue;
    private String writeQueue;
    private int messageCount;

    public TransformRequest() {
    }

    public String getReadQueue() {
        return readQueue;
    }

    public void setReadQueue(String readQueue) {
        this.readQueue = readQueue;
    }

    public String getWriteQueue() {
        return writeQueue;
    }

    public void setWriteQueue(String writeQueue) {
        this.writeQueue = writeQueue;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(int messageCount) {
        this.messageCount = messageCount;
    }
}