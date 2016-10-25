package org.iot.app.flink.kafka;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.iot.app.flink.model.AggregateKey;
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
        SingleOutputStreamOperator<IoTData> reduceKeyedStream = nonFilteredIotDataStream.keyBy(IotDataStreamUtils.iotDatakeySelector())
                                                                                        .reduce((a, b) -> a);
        // Paasing Keyed stream thorugh a stateful mapper to dedup duplicate event from same vehicle Id.
        SingleOutputStreamOperator<Tuple2<IoTData, Boolean>> dedupedKeyedStream = reduceKeyedStream.keyBy(IotDataStreamUtils.iotDatakeySelector())
                                                                                                   .map(new
                                                                                                           UniqueVehicleState());

        SingleOutputStreamOperator<Tuple2<IoTData, Boolean>> uniqueVehicleStreams = dedupedKeyedStream.filter(p -> p.f1);
        SingleOutputStreamOperator<IoTData> filteredIotDataStream = uniqueVehicleStreams
                .map(p -> p.f0);
        processTotalTrafficData(filteredIotDataStream);
        processWindowTrafficData(filteredIotDataStream);
        env.execute("IOT kafka cassandra sample");

    }

    public static void processTotalTrafficData(SingleOutputStreamOperator<IoTData> filteredIotDataStream) {
        SingleOutputStreamOperator<Tuple2<AggregateKey, Long>> groupedStrem =
                tokeyedStreamForCounting(filteredIotDataStream).reduce((a, b) -> new Tuple2(a.f0, a.f1.longValue() + b.f1.longValue()));

        groupedStrem.map(p -> {
            System.out.println("Total Count for Key::: " + p.f0.toString() + " Count:: " + p.f1.toString());
            return p;
        });
    }

    public static void processWindowTrafficData(SingleOutputStreamOperator<IoTData> filteredIotDataStream) {
        //Build a tumble Windowed keyedStream of 30 sec.
        WindowedStream<Tuple2<AggregateKey, Long>, AggregateKey, TimeWindow> tumbleWindowedStream =
                tokeyedStreamForCounting(filteredIotDataStream).timeWindow(Time.seconds(30));
        SingleOutputStreamOperator<Tuple2<AggregateKey, Long>> reducedWindowStream = tumbleWindowedStream.reduce((a, b) -> new Tuple2(a.f0, a.f1.longValue() + b.f1.longValue()));

        reducedWindowStream.map(p -> {
            System.out.println("Total Count  per Window for Key::: " + p.f0.toString() + " Count:: " + p.f1.toString
                    ());
            return p;
        });
    }

    private static KeyedStream<Tuple2<AggregateKey, Long>, AggregateKey> tokeyedStreamForCounting(SingleOutputStreamOperator<IoTData> filteredIotDataStream) {
        return filteredIotDataStream.map(p ->
                new Tuple2<AggregateKey, Long>(new AggregateKey(p.getRouteId(), p.getVehicleType()), new Long(1)))
                                    .keyBy
                                            (IotDataStreamUtils.AggregateKeySelector());
    }


}


