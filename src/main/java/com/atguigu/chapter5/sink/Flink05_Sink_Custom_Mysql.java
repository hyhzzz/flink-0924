package com.atguigu.chapter5.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author CoderHyh
 * @create 2022-03-29 14:51
 */
public class Flink05_Sink_Custom_Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1607527992000L, 20),
                new WaterSensor("sensor_1", 1607527994000L, 50),
                new WaterSensor("sensor_1", 1607527996000L, 50),
                new WaterSensor("sensor_2", 1607527993000L, 10),
                new WaterSensor("sensor_2", 1607527995000L, 30)
        );


        stream.keyBy(WaterSensor::getId)
                .sum("vc")
                .addSink(new MySqlSink());


        env.execute();
    }

    public static class MySqlSink extends RichSinkFunction<WaterSensor> {
        private Connection conn;
        private PreparedStatement ps;

        @Override
        public void open(Configuration parameters) throws Exception {
            //建立mysql的连接
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");

            //conn.setAutoCommit(false);
        }

        //在这个方法内实现具体的写入动作
        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {

            //通过连接对象得到一个预处理语句，PrepareStatement
            //第一次插入二次应该是更新，key重复
            //ps = conn.prepareStatement("insert into sensor values(?, ?,?,?)on  duplicate key update vc=? ");
            ps = conn.prepareStatement("replace into sensor values(?, ?,?)");
            //需要给占位符赋值
            ps.setString(1, value.getId());
            ps.setLong(2, value.getTs());
            ps.setInt(3, value.getVc());
            //ps.setInt(4,value.getVc());

            //执行写入
            ps.execute();

            //conn.commit();

            ps.close();

        }

        @Override
        public void close() throws Exception {

            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }
}
