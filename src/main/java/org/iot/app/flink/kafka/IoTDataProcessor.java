package org.iot.app.flink.kafka;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer081;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.iot.app.flink.model.IoTData;
import org.iot.app.flink.utils.IotDataStreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by harjags86 on 10/16/16.
 */
public class IoTDataProcessor {
    private static final Logger slf4jLogger = LoggerFactory.getLogger(IoTDataProcessor.class);

    public static void process(StreamExecutionEnvironment env,Properties properties) throws Exception {
        // set up the execution environment

        FlinkKafkaConsumer08<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer08<>("iot-data-event-flink",
                new JSONKeyValueDeserializationSchema(true),
                properties);

        kafkaConsumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ObjectNode>() {
            @Override
            public long extractAscendingTimestamp(ObjectNode jsonNodes) {
                return jsonNodes.get("timestamp")
                        .asLong();
            }
        });

        AllWindowedStream<ObjectNode, TimeWindow> windowedKafkStream = env.addSource(
                kafkaConsumer)
                .timeWindowAll(Time.seconds(5));
        processStream(windowedKafkStream);
        env.execute("IOT kafka cassandra sample");

    }


    private static void processStream(AllWindowedStream<ObjectNode, TimeWindow> kafkaStream) {
        //We need non filtered stream for poi traffic data calculation
        SingleOutputStreamOperator<IoTData> nonFilteredIotDataStream = kafkaStream.apply(IotDataStreamUtils.windowToDataStream());
        //We need filtered stream for total and traffic data calculation
        KeyedStream<Tuple2<String, IoTData>, Tuple> iotDataPairStream = nonFilteredIotDataStream.map(new IotDataStreamUtils.MapToPair())
                .keyBy(0);
        iotDataPairStream.print();
    }


}
