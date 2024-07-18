package com.padraigbutler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
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

public class DebeziumTransactionReconstructorWithEventCorrelation {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumTransactionReconstructor.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                .addSource(new FlinkKafkaConsumer<>("mysql-server.db_1.*", new SimpleStringSchema(), properties))
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

        // First stage: Correlate events with transaction metadata
        DataStream<CorrelatedEvent> correlatedStream = dataChangeStream
                .connect(transactionStream)
                .process(new EventCorrelator())
                .name("Event Correlator");

        // Second stage: Reconstruct complete transactions
        DataStream<TransactionRecord> reconstructedTransactions = correlatedStream
                .keyBy(CorrelatedEvent::getTransactionId)
                .window(GlobalWindows.create())
                .trigger(new TransactionCompletionTrigger())
                .process(new TransactionReconstructor())
                .name("Transaction Reconstructor")
                .uid("transaction-reconstruction")
                .setParallelism(4); // Adjust based on your needs

        reconstructedTransactions.print();

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

    public static class EventCorrelator extends CoProcessFunction<DebeziumChangeEvent, DebeziumTransactionEvent, CorrelatedEvent> {
        private MapState<String, TransactionMetadata> transactionMetadataState;
        private final OutputTag<String> invalidEventsOutput = new OutputTag<String>("invalid-events"){};

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, TransactionMetadata> descriptor =
                    new MapStateDescriptor<>("transactionMetadata", String.class, TransactionMetadata.class);
            transactionMetadataState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement1(DebeziumChangeEvent changeEvent, Context ctx, Collector<CorrelatedEvent> out) throws Exception {
            try {
                String transactionId = changeEvent.getTransactionId();
                TransactionMetadata metadata = transactionMetadataState.get(transactionId);
                if (metadata != null) {
                    out.collect(new CorrelatedEvent(changeEvent, metadata));
                } else {
                    // Buffer the event or handle the case where metadata hasn't arrived yet
                    LOG.warn("Received change event for unknown transaction: {}", transactionId);
                }
            } catch (Exception e) {
                LOG.error("Error processing change event: {}", changeEvent, e);
                ctx.output(invalidEventsOutput, "Error processing change event: " + changeEvent.toString());
            }
        }

        @Override
        public void processElement2(DebeziumTransactionEvent transactionEvent, Context ctx, Collector<CorrelatedEvent> out) throws Exception {
            try {
                transactionMetadataState.put(transactionEvent.getTransactionId(), new TransactionMetadata(transactionEvent));
            } catch (Exception e) {
                LOG.error("Error processing transaction event: {}", transactionEvent, e);
                ctx.output(invalidEventsOutput, "Error processing transaction event: " + transactionEvent.toString());
            }
        }
    }

    public static class TransactionCompletionTrigger extends Trigger<CorrelatedEvent, GlobalWindow> {
        @Override
        public TriggerResult onElement(CorrelatedEvent element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
            if (element.getMetadata().isComplete()) {
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {}
    }

    public static class TransactionReconstructor extends ProcessWindowFunction<CorrelatedEvent, TransactionRecord, String, GlobalWindow> {
        @Override
        public void process(String transactionId, Context context, Iterable<CorrelatedEvent> elements, Collector<TransactionRecord> out) throws Exception {
            List<DebeziumChangeEvent> changeEvents = new ArrayList<>();
            TransactionMetadata metadata = null;

            for (CorrelatedEvent event : elements) {
                changeEvents.add(event.getChangeEvent());
                if (metadata == null) {
                    metadata = event.getMetadata();
                }
            }

            if (metadata != null && metadata.isComplete()) {
                out.collect(new TransactionRecord(transactionId, changeEvents, metadata));
            }
        }
    }


    public static class CorrelatedEvent {
        private final DebeziumChangeEvent changeEvent;
        private final TransactionMetadata metadata;

        public CorrelatedEvent(DebeziumChangeEvent changeEvent, TransactionMetadata metadata) {
            this.changeEvent = changeEvent;
            this.metadata = metadata;
        }

        public DebeziumChangeEvent getChangeEvent() { return changeEvent; }
        public TransactionMetadata getMetadata() { return metadata; }
        public String getTransactionId() { return changeEvent.getTransactionId(); }
    }

    public static class TransactionMetadata {
        private final String status;
        private final long eventCount;

        public TransactionMetadata(DebeziumTransactionEvent event) {
            this.status = event.getStatus();
            this.eventCount = event.getEventCount();
        }

        public boolean isComplete() {
            return "END".equals(status);
        }
    }

    public static class TransactionRecord {
        private final String transactionId;
        private final List<DebeziumChangeEvent> events;
        private final TransactionMetadata metadata;

        public TransactionRecord(String transactionId, List<DebeziumChangeEvent> events, TransactionMetadata metadata) {
            this.transactionId = transactionId;
            this.events = new ArrayList<>(events);
            this.metadata = metadata;
        }

        public String getTransactionId() { return transactionId; }
        public List<DebeziumChangeEvent> getEvents() { return new ArrayList<>(events); }
        public TransactionMetadata getMetadata() { return metadata; }

        @Override
        public String toString() {
            return "TransactionRecord{" +
                    "transactionId='" + transactionId + '\'' +
                    ", eventCount=" + events.size() +
                    ", status='" + metadata.status + '\'' +
                    '}';
        }
    }
}