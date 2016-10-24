package org.iot.app.flink.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.flink.util.Collector;
import org.iot.app.flink.model.IoTData;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by harjags86 on 10/23/16.
 */
public class UniqueVehicleState extends RichMapFunction<IoTData, Tuple2<IoTData,Boolean>> {

    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private transient ListState<String> uniqueVehicleId;

    @Override
    public Tuple2<IoTData,Boolean> map(IoTData ioTData) throws Exception {
        if(!Iterables.contains(uniqueVehicleId.get(),ioTData.getVehicleId())){
            uniqueVehicleId.add(ioTData.getVehicleId());
            return new Tuple2(ioTData,true);
        }
        return new Tuple2(ioTData,false);
    }

    @Override
    public void open(Configuration config) {
        ListStateDescriptor<String> descriptor =
                new ListStateDescriptor<String>("unique_vehicle",String.class); // default value of the state, if
        // nothing was set
        uniqueVehicleId = getRuntimeContext().getListState(descriptor);
    }
}
