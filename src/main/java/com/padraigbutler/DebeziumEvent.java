package com.padraigbutler;

public abstract class DebeziumEvent {
    private final String transactionId;

    public DebeziumEvent(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getTransactionId() {
        return transactionId;
    }
}