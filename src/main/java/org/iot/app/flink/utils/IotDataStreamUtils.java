package org.iot.app.flink.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.iot.app.flink.model.IoTData;

import java.util.Date;
import java.util.TimeZone;

/**
 * Created by harjags86 on 10/17/16.
 */
public class IotDataStreamUtils {
    public static AllWindowFunction<ObjectNode, IoTData, TimeWindow> windowToDataStream() {
        return new AllWindowFunction<ObjectNode, IoTData, TimeWindow>() {
            public void apply(TimeWindow window,
                              Iterable<ObjectNode> values,
                              Collector<IoTData> collector) throws Exception {
                FastDateFormat formatter = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", TimeZone.getTimeZone("IST"));
                for (ObjectNode jsonNode : values) {
                    IoTData ioTData = new IoTData(jsonNode.get("vehicleId")
                            .asText(), jsonNode.get("vehicleType")
                            .asText(), jsonNode.get("routeId")
                            .asText(), jsonNode.get("latitude")
                            .asText(), jsonNode.get("longitude")
                            .asText(),
                            formatter.parse(jsonNode.get("timestamp")
                                    .asText()), jsonNode.get("speed")
                            .asDouble(), jsonNode.get("fuelLevel")
                            .asDouble());
                    collector.collect(ioTData);
                }
            }
        };
    }

    public static class MapToPair implements MapFunction<IoTData, Tuple2<String, IoTData>> {
        ObjectMapper mapper;

        @Override
        public Tuple2<String, IoTData> map(IoTData ioTData) throws Exception {
            return new Tuple2<>(ioTData.getVehicleId(), ioTData);
        }
    }
}
