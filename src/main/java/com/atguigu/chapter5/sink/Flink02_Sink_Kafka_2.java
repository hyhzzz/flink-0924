package com.atguigu.chapter5.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author CoderHyh
 * @create 2022-03-29 9:46
 */
public class Flink02_Sink_Kafka_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1607527992000L, 20),
                new WaterSensor("sensor_1", 1607527994000L, 50),
                new WaterSensor("sensor_1", 1607527996000L, 50),
                new WaterSensor("sensor_2", 1607527993000L, 10),
                new WaterSensor("sensor_2", 1607527995000L, 30)
        );

        //sensor_1的数据写到sensor_1topic中，sensor_2的数据写到sensor_2topic中
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092");

        stream.addSink(new FlinkKafkaProducer<WaterSensor>(
                "default", new KafkaSerializationSchema<WaterSensor>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(WaterSensor element,
                                                            @Nullable Long aLong) {
                return new ProducerRecord<>(
                        element.getId(), element.toString().getBytes(StandardCharsets.UTF_8));
            }
        }, props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        ));


        env.execute();
    }
}
