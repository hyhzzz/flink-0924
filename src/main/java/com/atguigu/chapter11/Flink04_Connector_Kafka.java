package com.atguigu.chapter11;

import com.ibm.icu.impl.Row;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author coderhyh
 * @create 2022-04-04 8:29
 */
class Flink04_Connector_Kafka {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //  创建表
        //  表的元数据信息
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        // 接文件, 并创建一个临时表, 其实就是一个动态表
        tableEnv
                .connect(new Kafka()
                        .version("universal")
                        .topic("sensor")
                        .startFromLatest()
                        .property("group.id", "bigdata")
                        .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092"))
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //  对动态表进行查询
        Table sensorTable = tableEnv.from("sensor");
        Table resultTable = sensorTable
                .groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));

        //  把动态表转换成流. 如果涉及到数据的更新, 要用到撤回流. 多个了一个boolean标记
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(resultTable, Row.class);
        resultStream.print();

        //启动执行环境
        env.execute();

    }
}
