package org.iot.app.flink.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.iot.app.flink.model.AggregateKey;
import org.iot.app.flink.model.IoTData;

import java.text.ParseException;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by harjags86 on 10/17/16.
 */
public class IotDataStreamUtils {
    private static final FastDateFormat formatter = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss",
            TimeZone.getTimeZone("IST"));

    public static MapFunction<ObjectNode, IoTData> streamToIotDataMap() {
        return new MapFunction<ObjectNode, IoTData>() {
            @Override
            public IoTData map(ObjectNode jsonNodes) throws Exception {
                return IotDataStreamUtils.jsonNodeToIotData(jsonNodes);
            }
        };
    }

    public static AscendingTimestampExtractor<ObjectNode> eventTimeStampExtractor() {
        return new AscendingTimestampExtractor<ObjectNode>() {
            @Override
            public long extractAscendingTimestamp(ObjectNode jsonNodes) {
                return jsonNodes.get("timestamp")
                                .asLong();
            }
        };
    }
    public static AllWindowFunction<ObjectNode, IoTData, TimeWindow> windowToDataStream() {
        return new AllWindowFunction<ObjectNode, IoTData, TimeWindow>() {
            public void apply(TimeWindow window,
                              Iterable<ObjectNode> values,
                              Collector<IoTData> collector) throws Exception {
                for (ObjectNode jsonNode : values) {
                    IoTData ioTData = jsonNodeToIotData(jsonNode);
                    collector.collect(ioTData);
                }
            }
        };
    }

    public static IoTData jsonNodeToIotData(ObjectNode jsonNode) throws ParseException {
        return new IoTData(jsonNode.get("vehicleId")
                                   .asText(), jsonNode.get("vehicleType")
                                                      .asText(), jsonNode.get("routeId")
                                                                         .asText(), jsonNode.get("latitude")
                                                                                            .asText(), jsonNode.get("longitude")
                                                                                                               .asText(),
                formatter.parse(jsonNode.get("timestamp")
                                        .asText()), jsonNode.get("speed")
                                                            .asDouble(), jsonNode.get("fuelLevel")
                                                                                 .asDouble());
    }

    public static class MapToPair implements MapFunction<IoTData, Tuple2<String, IoTData>> {
        ObjectMapper mapper;

        @Override
        public Tuple2<String, IoTData> map(IoTData ioTData) throws Exception {
            return new Tuple2<>(ioTData.getVehicleId(), ioTData);
        }
    }

    public static KeySelector<IoTData, String> iotDatakeySelector() {
        return new KeySelector<IoTData, String>() {
            @Override
            public String getKey(IoTData ioTData) throws Exception {
                return ioTData.getVehicleId();
            }
        };
    }
    public static KeySelector<Tuple2<AggregateKey, Long>, AggregateKey> AggregateKeySelector() {
        return new KeySelector<Tuple2<AggregateKey, Long>, AggregateKey>() {
            @Override
            public AggregateKey getKey
                    (Tuple2<AggregateKey, Long> input) throws Exception {
                return
                        input.f0;
            }
        };
    }


}
