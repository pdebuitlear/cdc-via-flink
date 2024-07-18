package com.padraigbutler;

import java.util.ArrayList;
import java.util.List;

public class TransactionState {
    private final String transactionId;
    private final List<DebeziumChangeEvent> events;

    public TransactionState(String transactionId) {
        this.transactionId = transactionId;
        this.events = new ArrayList<>();
    }

    public void addEvent(DebeziumChangeEvent event) {
        events.add(event);
    }

    public String getTransactionId() { return transactionId; }
    public List<DebeziumChangeEvent> getEvents() { return events; }
}