package com.atguigu.chapter8;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @author coderhyh
 * @create 2022-04-02 1:47
 */
class Flink01_Project_Product_PV {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] data = line.split(",");

                    return new UserBehavior(
                            Long.valueOf(data[0]),
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2]),
                            data[3],
                            Long.valueOf(data[4]) * 1000
                    );
                })
                .filter(ub -> "pv".equals(ub.getBehavior()))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<UserBehavior>() {
                                            @Override
                                            public long extractTimestamp(UserBehavior userBehavior, long l) {
                                                return userBehavior.getTimestamp();
                                            }
                                        }
                                ))
                .keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                //.aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                //    @Override
                //    public Long createAccumulator() {
                //        return 0L;
                //    }
                //
                //    @Override
                //    public Long add(UserBehavior userBehavior, Long acc) {
                //        return acc + 1;
                //    }
                //
                //    @Override
                //    public Long getResult(Long acc) {
                //        return acc;
                //    }
                //
                //    @Override
                //    public Long merge(Long a, Long b) {
                //        return a + b;
                //    }
                //}).print();

                .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<UserBehavior, String, String, TimeWindow>.Context context,
                                        Iterable<UserBehavior> elements,
                                        Collector<String> out) throws Exception {

                        UserBehavior result = elements.iterator().next();
                        Date start = new Date(context.window().getStart());
                        Date end = new Date(context.window().getEnd());

                        out.collect(start + " " + end + " " + result);

                    }
                }).print();


        //启动执行环境
        env.execute();
    }
}
