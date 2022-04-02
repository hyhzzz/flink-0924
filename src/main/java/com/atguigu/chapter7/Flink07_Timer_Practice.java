package com.atguigu.chapter7;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author CoderHyh
 * @create 2022-03-31 16:19
 */
public class Flink07_Timer_Practice {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop102", 9999)  // 在socket终端只输入毫秒级别的时间戳
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long l) {
                                return element.getTs();
                            }
                        }))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    boolean isFirst = true;
                    int lastVc;
                    private long ts;


                    @Override
                    public void processElement(WaterSensor value,
                                               KeyedProcessFunction<String, WaterSensor, String>
                                                       .Context context, Collector<String> collector) throws Exception {
                        /*
                        当一条数据来的时候，注册一个定时器5s点后触发定时器，
                        后面的数据如果是上升，则什么都不做
                        如果下降，则取消定时器，顺便注册一个新的定时器5s后触发的定时
                         */
                        if (isFirst) {
                            isFirst = false;

                            //注册定时器
                            long ts = value.getTs() + 5000;
                            context.timerService().registerEventTimeTimer(ts);


                        } else {
                            //不是第一条，判断这次的水位相比上次是否上升，如果上升，则什么都不做，如果没有上升，则删除对你搞事情

                            if (value.getVc() < lastVc) {

                                context.timerService().deleteEventTimeTimer(ts);

                                //重新注册新的定时器
                                long ts = value.getTs() + 5000;
                                context.timerService().registerEventTimeTimer(ts);
                            } else {
                                System.out.println("水位上升，什么都不做");
                            }
                        }
                        lastVc = value.getVc();
                    }


                    //当定时器触发的时候执行这个方法

                    /**
                     *
                     * @param timestamp 正在触发定时器的时间
                     * @param ctx  上下文
                     * @param out  输出数据
                     * @throws Exception
                     */
                    @Override
                    public void onTimer(long timestamp,
                                        KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx,
                                        Collector<String> out) throws Exception {

                        out.collect(ctx.getCurrentKey() + "水位5s内连续上升,红色预警");
                    }
                }).print();

        //启动执行环境
        env.execute();
    }
}
