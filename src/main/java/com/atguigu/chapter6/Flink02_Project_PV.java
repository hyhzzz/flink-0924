package com.atguigu.chapter6;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author CoderHyh
 * @create 2022-03-30 0:55
 */
public class Flink02_Project_PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .readTextFile("input/UserBehavior.csv")
                .map(line -> { // 对数据切割, 然后封装到POJO中
                    String[] split = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4]));
                })
                .filter(behavior -> "pv".equals(behavior.getBehavior())) //过滤出pv行为
                .keyBy(UserBehavior::getBehavior)
                //<String, UserBehavior, Long> key的类型  输入  输出
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                    long count = 0;

                    @Override
                    public void processElement(UserBehavior value,
                                               KeyedProcessFunction<String, UserBehavior, Long>.Context context,
                                               Collector<Long> collector) throws Exception {
                        count++;
                        collector.collect(count);

                    }
                }).print();

        env.execute();
    }
}
