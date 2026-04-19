package uk.ac.ed.inf.acp_submission_2.model;

public class CounterMessage {

    private String uid;
    private int counter;

    public CounterMessage() {
    }

    public CounterMessage(String uid, int counter) {
        this.uid = uid;
        this.counter = counter;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }
}