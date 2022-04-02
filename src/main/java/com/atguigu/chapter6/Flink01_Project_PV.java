package com.atguigu.chapter6;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CoderHyh
 * @create 2022-03-30 0:54
 */
public class Flink01_Project_PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("input/UserBehavior.csv")
                //把流中的数据封装成UserBehavior类型
                .map(line -> {
                    String[] data = line.split(",");
                    return new UserBehavior(Long.valueOf(data[0]),
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2]),
                            data[3],
                            Long.valueOf(data[4]));

                })
                .filter(ub -> "pv".equals(ub.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2 map(UserBehavior userBehavior) throws Exception {
                        return Tuple2.of("pv", 1L);
                    }
                }).keyBy(t -> t.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
