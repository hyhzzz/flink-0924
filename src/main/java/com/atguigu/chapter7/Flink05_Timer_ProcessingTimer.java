package com.atguigu.chapter7;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author CoderHyh
 * @create 2022-03-31 16:19
 */
public class Flink05_Timer_ProcessingTimer {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop102", 9999)  // 在socket终端只输入毫秒级别的时间戳
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value,
                                               KeyedProcessFunction<String, WaterSensor, String>
                                                       .Context context, Collector<String> collector) throws Exception {
                        // 处理时间过后5s后触发定时器
                        //注册处理时间定时器
                        if (value.getVc() > 20) {
                            long ts = System.currentTimeMillis() + 5000;
                            context.timerService()
                                    .registerProcessingTimeTimer(ts);
                        }
                        //如果水位小于10，取消掉定时器
                        //context.timerService().deleteProcessingTimeTimer();
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

                        out.collect(ctx.getCurrentKey() + "传感器检测到水位超过20,红色预警");
                    }
                }).print();

        //启动执行环境
        env.execute();
    }
}
