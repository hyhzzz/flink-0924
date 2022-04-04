package com.atguigu.chapter10;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.types.Row;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-04 8:48
 */
class Flink08_SQL_BaseUse_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 使用sql查询一个已注册的表
        // 1. 从流得到一个表
        Table inputTable = tableEnv.fromDataStream(waterSensorStream);
        // 2. 把注册为一个临时视图
        tableEnv.createTemporaryView("sensor", inputTable);
        // 3. 在临时视图查询数据, 并得到一个新表
        Table resultTable = tableEnv.sqlQuery("select * from sensor where id='sensor_1'");
        // 4. 显示resultTable的数据
        tableEnv.toAppendStream(resultTable, Row.class).print();
        env.execute();
    }
}
