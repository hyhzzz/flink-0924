package com.atguigu.chapter5.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author CoderHyh
 * @create 2022-03-29 0:04
 */
public class Flink03_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "Flink03_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");

        //DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));
        //kafkaSource.print();

        //读取json格式数据
        //env.addSource(new FlinkKafkaConsumer<Object>("sensor",new JSONKeyValueDeserializationSchema(false),properties)).print();

        env.execute();
    }
}
