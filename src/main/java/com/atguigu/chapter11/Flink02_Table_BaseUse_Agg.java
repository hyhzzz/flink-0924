package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author coderhyh
 * @create 2022-04-04 0:37
 */
class Flink02_Table_BaseUse_Agg {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(waterSensorStream);


        Table result = table.groupBy($("id"))
                .aggregate($("vc").sum().as("vc_sum"))
                .select($("id"), $("vc_sum"));


        //当有删除或者更新的时候使用撤回流
        //tableEnv.toRetractStream(result, Row.class).print();

        //idea端调式sql代码使用
        result.execute().print();

        //启动执行环境
        //env.execute();
    }
}
