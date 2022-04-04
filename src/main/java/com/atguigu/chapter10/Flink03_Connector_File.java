package com.atguigu.chapter10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author coderhyh
 * @create 2022-04-04 7:43
 */
class Flink03_Connector_File {
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

        //  连接文件, 并创建一个临时表, 其实就是一个动态表
        tableEnv.connect(new FileSystem().path("input/sensor.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //  做成表对象, 然后对动态表进行查询
        Table Table = tableEnv.from("sensor").select($("id"), $("vc"));

        //把表数据写入到文件中
        //建立一个动态表A与文件关联,然后把数据写入到动态表A中则自动写入到文件
        tableEnv.connect(new FileSystem().path("input/a.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("vc", DataTypes.INT()))
                .createTemporaryTable("s1");

        //tableEnv.insertInto("out", sensorTable);
        Table.executeInsert("s1");

        //  把动态表转换成流. 如果涉及到数据的更新, 要用到撤回流. 多个了一个boolean标记
        //DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(resultTable, Row.class);
        //resultStream.print();

        //resultTable.execute().print();

        //启动执行环境
        //env.execute();
    }
}
