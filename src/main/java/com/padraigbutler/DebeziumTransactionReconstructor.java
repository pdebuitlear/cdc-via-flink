package com.padraigbutler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DebeziumTransactionReconstructor {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumTransactionReconstructor.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Error handling and recovery
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        env.enableCheckpointing(60000); // Enable checkpointing every 60 seconds

        env.getConfig().setGlobalJobParameters(new Configuration() {{
            setString("pipeline.name", "Debezium CDC Transaction Reconstruction");
        }});

        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
            LOG.error("Uncaught exception in thread " + thread.getName(), throwable);
        });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("group.id", "flink-cdc-consumer");

        // Data change events stream
        DataStream<DebeziumChangeEvent> dataChangeStream = env
                .addSource(new FlinkKafkaConsumer<>("mysql-server.db_1.user_1", new SimpleStringSchema(), properties))
                .map(new DebeziumJsonParser())
                .filter(event -> event instanceof DebeziumChangeEvent)
                .map(event -> (DebeziumChangeEvent) event)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<DebeziumChangeEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );

        // Transaction metadata stream
        DataStream<DebeziumTransactionEvent> transactionStream = env
                .addSource(new FlinkKafkaConsumer<>("mysql-server.transaction", new SimpleStringSchema(), properties))
                .map(new DebeziumJsonParser())
                .filter(event -> event instanceof DebeziumTransactionEvent)
                .map(event -> (DebeziumTransactionEvent) event);

        // Connect the two streams
        DataStream<TransactionRecord> reconstructedTransactions = dataChangeStream
                .connect(transactionStream)
                .keyBy(
                        new KeySelector<DebeziumChangeEvent, String>() {
                            @Override
                            public String getKey(DebeziumChangeEvent event) throws Exception {
                                return event.getTransactionId();
                            }
                        },
                        new KeySelector<DebeziumTransactionEvent, String>() {
                            @Override
                            public String getKey(DebeziumTransactionEvent event) throws Exception {
                                return event.getTransactionId();
                            }
                        }
                )
                .process(new TransactionReconstructor())
                .name("Transaction Reconstructor")
                .uid("transaction-reconstruction")
                .setParallelism(4); //

        reconstructedTransactions.print();  // For demonstration; replace with your sink

        env.execute("Debezium CDC Transaction Reconstruction");
    }

    public static class DebeziumJsonParser implements MapFunction<String, DebeziumEvent> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public DebeziumEvent map(String value) throws Exception {
            try {
                JsonNode payload = objectMapper.readTree(value).path("payload");
                if (payload.isMissingNode()) {
                    LOG.warn("Received malformed event: {}", value);
                    return null;
                }

                if (payload.has("status")) {
                    // This is a transaction event
                    String status = payload.path("status").asText();
                    String id = payload.path("id").asText();
                    long eventCount = payload.path("event_count").asLong();
                    return new DebeziumTransactionEvent(id, status, eventCount);
                } else {
                    // This is a data change event
                    String op = payload.path("op").asText();
                    String transactionId = payload.path("transaction").path("id").asText();
                    long timestamp = payload.path("ts_ms").asLong();
                    JsonNode before = payload.path("before");
                    JsonNode after = payload.path("after");
                    return new DebeziumChangeEvent(op, transactionId, timestamp, before, after);
                }
            } catch (Exception e) {
                LOG.error("Error parsing event: {}", value, e);
                return null;
            }
        }
    }

    public static class TransactionReconstructor extends KeyedCoProcessFunction<String, DebeziumChangeEvent, DebeziumTransactionEvent, TransactionRecord> {
        private ValueState<TransactionState> transactionState;
        private final OutputTag<String> invalidEventsOutput = new OutputTag<String>("invalid-events"){};

        @Override
        public void open(Configuration parameters) throws Exception {
            transactionState = getRuntimeContext().getState(new ValueStateDescriptor<>("transaction-state", TransactionState.class));
        }

        @Override
        public void processElement1(DebeziumChangeEvent changeEvent, Context ctx, Collector<TransactionRecord> out) throws Exception {
            try {
                TransactionState state = transactionState.value();
                if (state == null) {
                    state = new TransactionState(changeEvent.getTransactionId());
                }
                state.addEvent(changeEvent);
                transactionState.update(state);
            } catch (Exception e) {
                LOG.error("Error processing change event: {}", changeEvent, e);
                ctx.output(invalidEventsOutput, "Error processing change event: " + changeEvent.toString());
            }
        }

        @Override
        public void processElement2(DebeziumTransactionEvent transEvent, Context ctx, Collector<TransactionRecord> out) throws Exception {
            try {
                if ("END".equals(transEvent.getStatus())) {
                    TransactionState state = transactionState.value();
                    if (state != null) {
                        out.collect(new TransactionRecord(state.getTransactionId(), state.getEvents(), transEvent));
                        transactionState.clear();
                    }
                }
            } catch (Exception e) {
                LOG.error("Error processing transaction event: {}", transEvent, e);
                ctx.output(invalidEventsOutput, "Error processing transaction event: " + transEvent.toString());
            }
        }
    }

    public static class TransactionState {
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

    public static class TransactionRecord {
        private final String transactionId;
        private final List<DebeziumChangeEvent> events;
        private final DebeziumTransactionEvent transactionEvent;

        public TransactionRecord(String transactionId, List<DebeziumChangeEvent> events, DebeziumTransactionEvent transactionEvent) {
            this.transactionId = transactionId;
            this.events = new ArrayList<>(events);
            this.transactionEvent = transactionEvent;
        }

        public String getTransactionId() { return transactionId; }
        public List<DebeziumChangeEvent> getEvents() { return new ArrayList<>(events); }
        public DebeziumTransactionEvent getTransactionEvent() { return transactionEvent; }

        @Override
        public String toString() {
            return "TransactionRecord{" +
                    "transactionId='" + transactionId + '\'' +
                    ", eventCount=" + events.size() +
                    ", status='" + transactionEvent.getStatus() + '\'' +
                    ", events='" + events+
                    '}';
        }
    }
}