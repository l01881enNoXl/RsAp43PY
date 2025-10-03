// 代码生成时间: 2025-10-04 00:00:26
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.javaStreamingContext.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;

public class StreamingMediaPlayer {
    public static void main(String[] args) {
        // Define the Kafka parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(