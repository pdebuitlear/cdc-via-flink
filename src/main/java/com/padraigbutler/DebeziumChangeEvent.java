package com.padraigbutler;

import com.fasterxml.jackson.databind.JsonNode;

public class DebeziumChangeEvent extends DebeziumEvent {
    private final String operation;
    private final long timestamp;
    private final JsonNode before;
    private final JsonNode after;

    public DebeziumChangeEvent(String operation, String transactionId, long timestamp, JsonNode before, JsonNode after) {
        super(transactionId);
        this.operation = operation;
        this.timestamp = timestamp;
        this.before = before;
        this.after = after;
    }

    // Getters
    public String getOperation() { return operation; }
    public long getTimestamp() { return timestamp; }
    public JsonNode getBefore() { return before; }
    public JsonNode getAfter() { return after; }

    @Override
    public String toString() {
        return "DebeziumChangeEvent{" +
                "transactionId='" + this.getTransactionId() + '\'' +
                ", operation='" + this.getOperation() + '\'' +
                ", after=" + this.getAfter() +
                '}';
    }
}