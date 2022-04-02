package com.atguigu.chapter8;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author coderhyh
 * @create 2022-04-02 10:23
 */
class Flink04_Project_AdsClick {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建WatermarkStrategy
        WatermarkStrategy<AdsClickLog> wms = WatermarkStrategy
                .<AdsClickLog>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((SerializableTimestampAssigner<AdsClickLog>) (element, recordTimestamp) -> element.getTimestamp() * 1000L);
        env
                .readTextFile("input/AdClickLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new AdsClickLog(
                            Long.parseLong(datas[0]),
                            Long.parseLong(datas[1]),
                            datas[2],
                            datas[3],
                            Long.valueOf(datas[4]));
                })
                .assignTimestampsAndWatermarks(wms)
                //每个用户对每个广告的点击率
                .keyBy(log -> log.getUserId() + "_" + log.getAdsId())
                .process(new KeyedProcessFunction<String, AdsClickLog, String>() {

                    private ValueState<String> yesterdayState;
                    private ValueState<Boolean> blackListState;
                    private ReducingState<Long> clickCountState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        clickCountState = getRuntimeContext().getReducingState(
                                new ReducingStateDescriptor<Long>(
                                        "clickCountState", (ReduceFunction<Long>) Long::sum, Long.class));

                        blackListState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("blackListState", Boolean.class));

                        yesterdayState = getRuntimeContext().getState(new ValueStateDescriptor<String>("yesterdayState", String.class));
                    }

                    @Override
                    public void processElement(AdsClickLog value,
                                               KeyedProcessFunction<String, AdsClickLog, String>.Context ctx,
                                               Collector<String> out) throws Exception {

                        //一但跨天，状态应该重置
                        String yesterday = yesterdayState.value();

                        String today = new SimpleDateFormat("yyyy-MM-dd").format(value.getTimestamp());
                        if (!today.equals(yesterday)) { //表示数据进入到第二天
                            yesterdayState.update(today); //把今天的日期存入到状态，明天就可以读到今天是昨天
                            clickCountState.clear();
                            blackListState.clear();
                        }

                        //如果已经进入了黑名单，则不需要在进行统计
                        if (blackListState.value() == null) {
                            clickCountState.add(1L);
                        }

                        Long count = clickCountState.get();
                        String msg = "用户" + value.getUserId() + "对广告:" + value.getAdsId() + "的点击量是:" + count;
                        if (count > 99) {
                            //第一次超过99加入黑名单，后面就不用再加入了
                            if (blackListState.value() == null) {
                                msg += "超过阈值99，加入黑名单";
                                out.collect(msg);
                                blackListState.update(true);
                            }
                        } else {
                            out.collect(msg);
                        }
                    }
                }).print();


        //启动执行环境
        env.execute();
    }
}
