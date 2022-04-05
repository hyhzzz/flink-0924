package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @author coderhyh
 * @create 2022-04-04 10:18
 */
class Flink13_Window_Grouped_1 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(), $("vc"));

        //1.在table api中使用窗口

        table
                //.window(Tumble.over(lit(10).second()).on($("ts")).as("w"))  // 定义滚动窗口并给窗口起一个别名
                //.window(Slide.over(lit(10).second()).every(lit(5).second()).on($("ts")).as("w")) //滑动窗口
                .window(Session.withGap(lit(2).second()).on($("ts")).as("w"))// session窗口
                .groupBy($("id"), $("w"))
                .select($("id"), $("w").start(), $("w").end(), $("vc").sum())
                .execute().print();


        //2.在sql语句中使用窗口


    }
}
