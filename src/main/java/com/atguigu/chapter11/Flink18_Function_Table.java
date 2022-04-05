package com.atguigu.chapter11;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author coderhyh
 * @create 2022-04-05 10:26
 */
class Flink18_Function_Table {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> stream = env.fromElements("hello,atguigu,world", "aaa,bbbbb", "");

        Table table = tEnv.fromDataStream(stream, $("line"));

        // 1. 内联使用
        //table
        //        .joinLateral(call(Split.class, $("line"))) //默认是内连接
        //        .select($("line"), $("word"), $("len"))
        //        .execute()
        //        .print();

        //table
        //        .leftOuterJoinLateral(call(Split.class, $("line"))) //左连接
        //        .select($("line"), $("word"), $("len"))
        //        .execute()
        //        .print();


        // 2. 注册后使用
        //tEnv.createTemporaryFunction("split", Split.class);
        //table
        //        .joinLateral(call("split", $("line")))
        //        .select($("line"), $("word"), $("len"))
        //        .execute()
        //        .print();


        //在sql中使用
        tEnv.createTemporaryView("t_word", table);
        // 2. 注册函数
        tEnv.createTemporaryFunction("split", Split.class);
        // 3. 使用函数
        // 3.1 join
        //tEnv.sqlQuery("select " +
        //        " line, word, len " +
        //        "from t_word " +
        //        "join lateral table(split(line)) on true").execute().print();
        // 或者
        tEnv.sqlQuery("select " +
                " line, word, len " +
                "from t_word, " +
                "lateral table(split(line))").execute().print();
        // 3.2 left join
        //tEnv.sqlQuery("select " +
        //        " line, word, len " +
        //        "from t_word " +
        //        "left join lateral table(split(line)) on true").execute().print();
        // 3.3 join或者left join给字段重命名
        tEnv.sqlQuery("select " +
                " line, new_word, new_len " +
                "from t_word " +
                "left join lateral table(split(line)) as T(new_word, new_len) on true").execute().print();

    }

    //row是一种弱类型，需要明确指定字段名和类型
    //row用来表示制成的表的每行数据的封装，也可以用pojo
    @FunctionHint(output = @DataTypeHint("ROW(word string, len int)"))
    public static class Split extends TableFunction<Row> {
        public void eval(String line) {
            if (line.length() == 0) {
                return;
            }
            for (String s : line.split(",")) {
                // 来一个字符串, 按照逗号分割, 得到多行, 每行为这个单词和他的长度
                collect(Row.of(s, s.length())); //调用一次，就有一行数据
            }
        }
    }


    //pojo是一种强类型，每个字段的类型和名字都是和pojo中的属性保持一致
    //public static class Split extends TableFunction<WordLen> {
    //    public void eval(String line) {
    //        for (String word : line.split(",")) {
    //            // 来一个字符串, 按照逗号分割, 得到多行, 每行为这个单词和他的长度
    //            collect(new WordLen(word, word.length())); //调用一次，就有一行数据
    //        }
    //    }
    //}
}
