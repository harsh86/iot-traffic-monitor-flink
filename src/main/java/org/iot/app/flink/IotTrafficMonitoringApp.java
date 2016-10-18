package org.iot.app.flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.iot.app.flink.kafka.IoTDataProcessor;

import java.util.Properties;

/**
 * Created by harjags86 on 10/18/16.
 */
public class IotTrafficMonitoringApp {


    private static Properties buildKafkaConfiguration() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "iot-data-event");
        return properties;
    }

    private static StreamExecutionEnvironment buildExecutionContext() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig()
                .disableSysoutLogging();
        env.getConfig()
                .setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 secodns
        return env;
    }

    public static void main(String args[]) {
        try {
            IoTDataProcessor.process(buildExecutionContext(),buildKafkaConfiguration());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
