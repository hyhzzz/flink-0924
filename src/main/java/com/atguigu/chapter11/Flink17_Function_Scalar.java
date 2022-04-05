package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author coderhyh
 * @create 2022-04-05 9:24
 */
class Flink17_Function_Scalar {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        Table table = tableEnv.fromDataStream(waterSensorStream);
        tableEnv.createTemporaryView("sensor", table);


        //使用自定义函数
        //1.在table api中使用
        //1.1 内联的方式使用
        //table.select($("id"), call(MyToUpperCase.class, $("id")).as("my_upper")).execute().print();

        //1.2 先注册后使用
        //tableEnv.createTemporaryFunction("my_upper", MyToUpperCase.class);
        //table.select($("id"), call("my_upper", $("id")).as("my_upper")).execute().print();


        //2.在sql中使用
        //先注册
        tableEnv.createTemporaryFunction("toUpper", MyToUpperCase.class);

        tableEnv.sqlQuery("select id, my_upper(id) id1 from sensor").execute().print();

    }

    public static class MyToUpperCase extends ScalarFunction {
        //方法名必须是eval
        public String eval(String s) {
            return s.toUpperCase();
        }
    }
}
