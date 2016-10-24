package org.iot.app.flink.kafka;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.iot.app.flink.model.IoTData;
import org.iot.app.flink.utils.IotDataStreamUtils;
import org.iot.app.flink.utils.UniqueVehicleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by harjags86 on 10/16/16.
 */
public class IoTDataProcessor {
    private static final Logger slf4jLogger = LoggerFactory.getLogger(IoTDataProcessor.class);

    public static void process(StreamExecutionEnvironment env, Properties properties) throws Exception {
        // set up the execution environment

        FlinkKafkaConsumer08<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer08<>("iot-data-event-flink",
                new JSONDeserializationSchema(),
                properties);

        // Source and Map events to IotData.
        SingleOutputStreamOperator<IoTData> nonFilteredIotDataStream = env.addSource(kafkaConsumer)
                                                                          .map(IotDataStreamUtils.streamToIotDataMap());
        //partition Stream by vehicle Id for deduplication.
        KeyedStream<IoTData, String> nonFilteredKeyedStream = nonFilteredIotDataStream.keyBy(IotDataStreamUtils
                .iotDatakeySelector());
        // Paasing Keyed stream thorugh a stateful mapper to dedup duplicate event from same vehicle Id.
        SingleOutputStreamOperator<Tuple2<IoTData, Boolean>> dedupedKeyedStream = nonFilteredKeyedStream.map(new
                UniqueVehicleState());

        SingleOutputStreamOperator<IoTData> filteredIotDataStream = dedupedKeyedStream.filter(p -> p.f1)
                                                                                      .map(p -> p.f0);
        env.execute("IOT kafka cassandra sample");

    }


}
