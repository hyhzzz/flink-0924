package com.atguigu.chapter10;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-04 8:44
 */
class Flink07_SQL_BaseUse {
    public static void main(String[] args) {
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
        // 使用sql查询未注册的表
        Table table = tableEnv.fromDataStream(waterSensorStream);
        tableEnv.sqlQuery("select * from " + table + " where id='sensor_1'").execute().print();

        //Table resultTable = tableEnv.sqlQuery("select * from " + inputTable + " where id='sensor_1'");
        //tableEnv.toAppendStream(resultTable, Row.class).print();


    }
}
