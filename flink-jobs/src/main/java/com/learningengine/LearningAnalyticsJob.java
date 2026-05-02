package com.learningengine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class LearningAnalyticsJob {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("user_activity")
                .setGroupId("flink-analytics-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "Kafka Source");

        DataStream<JsonNode> parsedStream = stream.map(new MapFunction<String, JsonNode>() {
            @Override
            public JsonNode map(String value) throws Exception {
                return mapper.readTree(value);
            }
        });

        // Sliding Window (Last 2 minutes, sliding every 30 seconds) for real-time engagement alerting
        DataStream<String> realTimeAlerts = parsedStream
                .keyBy(new KeySelector<JsonNode, String>() {
                    @Override
                    public String getKey(JsonNode value) throws Exception {
                        return value.get("user_id").asText();
                    }
                })
                .window(SlidingProcessingTimeWindows.of(Time.minutes(2), Time.seconds(30)))
                .apply(new WindowFunction<JsonNode, String, String, TimeWindow>() {
                    @Override
                    public void apply(String userId, TimeWindow window, Iterable<JsonNode> input, Collector<String> out) throws Exception {
                        int eventCount = 0;
                        for (JsonNode event : input) {
                            eventCount++;
                        }
                        if (eventCount < 2) {
                            out.collect(String.format("{\"user_id\":\"%s\", \"alert\":\"Low engagement detected in last 2 mins\", \"type\":\"alert\"}", userId));
                        }
                    }
                });

        // Tumbling Window (5 minutes) for deep analytics
        DataStream<String> analyticsStream = parsedStream
                .keyBy(new KeySelector<JsonNode, String>() {
                    @Override
                    public String getKey(JsonNode value) throws Exception {
                        return value.get("user_id").asText();
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .apply(new WindowFunction<JsonNode, String, String, TimeWindow>() {
                    @Override
                    public void apply(String userId, TimeWindow window, Iterable<JsonNode> input, Collector<String> out) throws Exception {
                        int totalQuizzes = 0;
                        int correctQuizzes = 0;
                        int totalTimeSpent = 0;
                        int interactionCount = 0;
                        String lastTopic = "";

                        for (JsonNode event : input) {
                            interactionCount++;
                            String eventType = event.has("event_type") ? event.get("event_type").asText() : "";
                            if (event.has("topic")) {
                                lastTopic = event.get("topic").asText();
                            }

                            if ("quiz_result".equals(eventType)) {
                                totalQuizzes++;
                                if (event.has("is_correct") && event.get("is_correct").asBoolean()) {
                                    correctQuizzes++;
                                }
                            } else if ("time_spent".equals(eventType)) {
                                if (event.has("duration")) {
                                    totalTimeSpent += event.get("duration").asInt();
                                }
                            }
                        }

                        double accuracy = totalQuizzes > 0 ? ((double) correctQuizzes / totalQuizzes) * 100.0 : 0.0;
                        int engagementScore = interactionCount * 10 + totalTimeSpent / 60; // simple metric

                        String weakTopic = "";
                        if (totalQuizzes > 0 && accuracy < 50.0) {
                            weakTopic = lastTopic;
                        }

                        // JSON Output
                        String result = String.format(
                                "{\"user_id\": \"%s\", \"accuracy\": %.2f, \"time_spent\": %d, \"engagement_score\": %d, \"weak_topic\": \"%s\", \"type\": \"analytics\"}",
                                userId, accuracy, totalTimeSpent, engagementScore, weakTopic
                        );
                        out.collect(result);
                    }
                });

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("processed_insights")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        analyticsStream.sinkTo(sink);
        realTimeAlerts.sinkTo(sink);

        env.execute("Learning Analytics Job");
    }
}
