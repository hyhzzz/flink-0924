package com.atguigu.chapter8;

import com.atguigu.bean.UserBehavior;
import com.atguigu.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-02 8:25
 */
class Flink02_Project_Product_UV {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("input/UserBehavior.csv")
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
                .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {

                    private MapState<Long, Object> userIdState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        userIdState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<Long, Object>("userIdState", Long.class, Object.class));
                    }

                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<UserBehavior, String, String, TimeWindow>.Context context,
                                        Iterable<UserBehavior> elements,
                                        Collector<String> out) throws Exception {

                        userIdState.clear();

                        for (UserBehavior element : elements) {
                            userIdState.put(element.getUserId(), new Object());
                        }
                        List<Long> userIdList = AtguiguUtil.toList(userIdState.keys());


                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String start = sdf.format(context.window().getStart());
                        String end = sdf.format(context.window().getEnd());
                        out.collect(start + "" + end + "uv：" + userIdList.size());
                    }
                }).print();


        //启动执行环境
        env.execute();
    }
}
