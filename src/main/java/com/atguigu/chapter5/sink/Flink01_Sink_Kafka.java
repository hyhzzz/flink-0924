package com.atguigu.chapter5.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

/**
 * @author CoderHyh
 * @create 2022-03-29 9:46
 */
public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.socketTextStream("hadoop102", 9999)
                .flatMap(
                        new FlatMapFunction<String, String>() {
                            @Override
                            public void flatMap(String s, Collector<String> collector) throws Exception {
                                for (String word : s.split(" ")) {
                                    collector.collect(word);
                                }
                            }
                        }
                )
                .addSink(new FlinkKafkaProducer<String>(
                        "hadoop102:9092,hadoop103:9092", "sensor", new SimpleStringSchema()));

        env.execute();
    }
}
