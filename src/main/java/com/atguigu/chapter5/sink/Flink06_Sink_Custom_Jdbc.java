package com.atguigu.chapter5.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author CoderHyh
 * @create 2022-03-29 14:51
 */
public class Flink06_Sink_Custom_Jdbc {
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
                .addSink(JdbcSink.sink("replace into sensor values(?,?,?)", new JdbcStatementBuilder<WaterSensor>() {
                            //流中没来一条数据，回调这个方法
                            @Override
                            public void accept(PreparedStatement ps, WaterSensor value) throws SQLException {
                                ps.setString(1, value.getId());
                                ps.setLong(2, value.getTs());
                                ps.setInt(3, value.getVc());
                            }
                        }, JdbcExecutionOptions.builder()
                                //执行参数
                                .withBatchIntervalMs(3000)
                                .withBatchSize(16 * 1024)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                //连接参数
                                .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("123456")
                                .build()
                ));


        env.execute();
    }

}
