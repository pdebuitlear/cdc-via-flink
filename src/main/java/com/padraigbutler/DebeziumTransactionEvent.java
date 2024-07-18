package com.padraigbutler;

public class DebeziumTransactionEvent extends DebeziumEvent {
    private final String status;
    private final long eventCount;

    public DebeziumTransactionEvent(String transactionId, String status, long eventCount) {
        super(transactionId);
        this.status = status;
        this.eventCount = eventCount;
    }

    public String getStatus() { return status; }
    public long getEventCount() { return eventCount; }

    @Override
    public String toString() {
        return "DebeziumTransactionEvent{" +
                "transactionId='" + this.getTransactionId() + '\'' +
                ", status=" + this.getStatus() +
                ", eventCount=" + this.getEventCount() +
                '}';
    }
}