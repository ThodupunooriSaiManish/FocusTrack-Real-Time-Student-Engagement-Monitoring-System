package com.focustrack;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FocusJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("focus-events")
                .setGroupId("focus-flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<FocusEvent> eventStream = rawStream.map(new ParseEventMap());

        // 1. Student-Level Intelligence (with At-Risk & Trend Detection)
        DataStream<String> studentStream = eventStream
                .keyBy(event -> event.userId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new FocusAggregator())
                .keyBy(json -> {
                    try { return new ObjectMapper().readTree(json).get("user_id").asText(); }
                    catch (Exception e) { return "unknown"; }
                })
                .process(new IntelligenceProcessFunction("STUDENT"));

        // 2. Section-Level Intelligence (with Reasons & Focus Index)
        DataStream<String> sectionStream = eventStream
                .keyBy(event -> event.department + "_" + event.year + "_" + event.section)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new SectionAggregator())
                .keyBy(json -> {
                    try { return new ObjectMapper().readTree(json).get("section_id").asText(); }
                    catch (Exception e) { return "unknown"; }
                })
                .process(new IntelligenceProcessFunction("SECTION"));

        // 3. Department-Level Analytics
        DataStream<String> deptStream = eventStream
                .keyBy(event -> event.department)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new DepartmentAggregator());

        // 4. Intelligent Alerts
        DataStream<String> alertStream = sectionStream
                .flatMap(new AlertIntelligenceFlatMap());

        DataStream<String> resultStream = studentStream.union(sectionStream, deptStream, alertStream);

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("focus-output")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        resultStream.sinkTo(sink);
        env.execute("FocusTrack Intelligence Platform");
    }

    public static class FocusEvent {
        public String userId, department, section, mentorId;
        public int year;
        public long timeSpent;
        public boolean correct;
    }

    public static class ParseEventMap implements MapFunction<String, FocusEvent> {
        private transient ObjectMapper mapper;
        @Override
        public FocusEvent map(String value) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(value);
            FocusEvent event = new FocusEvent();
            event.userId = node.has("user_id") ? node.get("user_id").asText() : "unknown";
            event.department = node.has("department") ? node.get("department").asText() : "UNKNOWN";
            event.year = node.has("year") ? node.get("year").asInt() : 1;
            event.section = node.has("section") ? node.get("section").asText() : "A";
            event.mentorId = node.has("mentor_id") ? node.get("mentor_id").asText() : "unknown";
            event.timeSpent = node.has("time_spent") ? node.get("time_spent").asLong() : 0;
            event.correct = node.has("correct") && node.get("correct").asBoolean();
            return event;
        }
    }

    public static class FocusAccumulator { String userId; int total = 0; int correct = 0; long totalTime = 0; }

    public static class FocusAggregator implements AggregateFunction<FocusEvent, FocusAccumulator, String> {
        @Override
        public FocusAccumulator createAccumulator() { return new FocusAccumulator(); }
        @Override
        public FocusAccumulator add(FocusEvent value, FocusAccumulator acc) {
            acc.userId = value.userId; acc.total++; if (value.correct) acc.correct++;
            acc.totalTime += value.timeSpent;
            return acc;
        }
        @Override
        public String getResult(FocusAccumulator acc) {
            double accuracy = acc.total == 0 ? 0 : (double) acc.correct / acc.total;
            double avgTime = acc.total == 0 ? 0 : (double) acc.totalTime / acc.total;
            double engagement = Math.min(1.0, avgTime / 120.0);
            
            int focusScore = (int) ((accuracy * 70) + (engagement * 30));
            // Ensure variation and realistic bounds
            focusScore = Math.max(20, Math.min(100, focusScore));
            
            String status = focusScore >= 70 ? "HIGH_FOCUS" : (focusScore >= 40 ? "MEDIUM_FOCUS" : "LOW_FOCUS");
            return String.format("{\"type\": \"STUDENT\", \"user_id\": \"%s\", \"accuracy\": %.2f, \"focus_score\": %d, \"status\": \"%s\"}", 
                acc.userId, accuracy, focusScore, status);
        }
        @Override
        public FocusAccumulator merge(FocusAccumulator a, FocusAccumulator b) {
            a.total += b.total; a.correct += b.correct; a.totalTime += b.totalTime; return a;
        }
    }

    public static class SectionAggregator implements AggregateFunction<FocusEvent, SectionAccumulator, String> {
        @Override
        public SectionAccumulator createAccumulator() { return new SectionAccumulator(); }
        @Override
        public SectionAccumulator add(FocusEvent value, SectionAccumulator acc) {
            acc.dept = value.department; acc.yr = value.year; acc.sec = value.section; acc.mentor = value.mentorId;
            acc.total++; if (value.correct) acc.correct++;
            double curAcc = value.correct ? 1.0 : 0.0;
            if (curAcc >= 0.7) acc.high++; else if (curAcc >= 0.4) acc.med++; else acc.low++;
            return acc;
        }
        @Override
        public String getResult(SectionAccumulator acc) {
            double focusIndex = acc.total == 0 ? 0 : (double) acc.correct / acc.total;
            return String.format("{\"type\": \"SECTION_ANALYTICS\", \"section_id\": \"%s-%d-%s\", \"department\": \"%s\", \"mentor_id\": \"%s\", \"focus_index\": %.2f, \"distribution\": {\"high\": %d, \"medium\": %d, \"low\": %d}}",
                acc.dept, acc.yr, acc.sec, acc.dept, acc.mentor, focusIndex, acc.high, acc.med, acc.low);
        }
        @Override
        public SectionAccumulator merge(SectionAccumulator a, SectionAccumulator b) {
            a.total += b.total; a.correct += b.correct; a.high += b.high; a.med += b.med; a.low += b.low; return a;
        }
    }
    public static class SectionAccumulator { String dept, sec, mentor; int yr, total, correct, high, med, low; }

    public static class DepartmentAggregator implements AggregateFunction<FocusEvent, DeptAccumulator, String> {
        @Override
        public DeptAccumulator createAccumulator() { return new DeptAccumulator(); }
        @Override
        public DeptAccumulator add(FocusEvent value, DeptAccumulator acc) {
            acc.dept = value.department; acc.total++; if (value.correct) acc.correct++;
            return acc;
        }
        @Override
        public String getResult(DeptAccumulator acc) {
            double focusIndex = acc.total == 0 ? 0 : (double) acc.correct / acc.total * 100;
            return String.format("{\"type\": \"DEPARTMENT_ANALYTICS\", \"department\": \"%s\", \"focus_index\": %d}", acc.dept, (int)focusIndex);
        }
        @Override
        public DeptAccumulator merge(DeptAccumulator a, DeptAccumulator b) {
            a.total += b.total; a.correct += b.correct; return a;
        }
    }
    public static class DeptAccumulator { String dept; int total, correct; }

    public static class IntelligenceProcessFunction extends KeyedProcessFunction<String, String, String> {
        private String type;
        private transient ValueState<Double> lastMetric;
        private transient ValueState<Integer> lowConsecutiveCount;
        private transient ObjectMapper mapper;

        public IntelligenceProcessFunction(String type) { this.type = type; }

        @Override
        public void open(Configuration parameters) {
            lastMetric = getRuntimeContext().getState(new ValueStateDescriptor<>("lastM", Double.class));
            lowConsecutiveCount = getRuntimeContext().getState(new ValueStateDescriptor<>("lowC", Integer.class));
            mapper = new ObjectMapper();
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            JsonNode node = mapper.readTree(value);
            double currentMetric = node.has("accuracy") ? node.get("accuracy").asDouble() : node.get("focus_index").asDouble();
            Double prevMetric = lastMetric.value();
            String trend = (prevMetric == null) ? "STABLE" : (currentMetric > prevMetric ? "IMPROVING" : (currentMetric < prevMetric ? "DECLINING" : "STABLE"));
            lastMetric.update(currentMetric);

            Map<String, Object> map = mapper.convertValue(node, Map.class);
            map.put("trend", trend);

            if (type.equals("STUDENT")) {
                int count = (currentMetric < 0.4) ? (lowConsecutiveCount.value() == null ? 1 : lowConsecutiveCount.value() + 1) : 0;
                lowConsecutiveCount.update(count);
                if (count >= 2) map.put("is_at_risk", true);
            } else if (type.equals("SECTION")) {
                List<String> reasons = new ArrayList<>();
                if (currentMetric < 0.5) reasons.add("Low Focus Index");
                if (trend.equals("DECLINING")) reasons.add("Performance Declining");
                map.put("reasons", reasons);
            }

            out.collect(mapper.writeValueAsString(map));
        }
    }

    public static class AlertIntelligenceFlatMap implements FlatMapFunction<String, String> {
        private transient ObjectMapper mapper;
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(value);
            double index = node.get("focus_index").asDouble();
            String trend = node.get("trend").asText();
            if (index < 0.4 || trend.equals("DECLINING")) {
                String severity = (index < 0.3) ? "CRITICAL" : "WARNING";
                String msg = String.format("Section %s focus index is %s with %s trend.", node.get("section_id").asText(), (int)(index*100)+"%", trend);
                out.collect(String.format("{\"type\": \"MENTOR_ALERT\", \"mentor_id\": \"%s\", \"severity\": \"%s\", \"message\": \"%s\", \"section\": \"%s\"}", 
                    node.get("mentor_id").asText(), severity, msg, node.get("section_id").asText()));
            }
        }
    }
}
