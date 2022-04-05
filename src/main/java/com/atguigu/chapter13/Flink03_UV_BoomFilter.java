package com.atguigu.chapter13;

import com.atguigu.bean.UserBehavior;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author coderhyh
 * @create 2022-04-05 21:07
 */
class Flink03_UV_BoomFilter {
   public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      // 创建WatermarkStrategy
      WatermarkStrategy<UserBehavior> wms = WatermarkStrategy
              .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
              .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                 @Override
                 public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                    return element.getTimestamp() * 1000L;
                 }
              });

      env
              .readTextFile("input/UserBehavior.csv")
              .map(line -> { // 对数据切割, 然后封装到POJO中
                 String[] split = line.split(",");
                 return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
              })
              .filter(behavior -> "pv".equals(behavior.getBehavior())) //过滤出pv行为
              .assignTimestampsAndWatermarks(wms)
              .keyBy(UserBehavior::getBehavior)
              .window(TumblingEventTimeWindows.of(Time.minutes(60)))
              .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {

                 private ValueState<Long> countState;
                 private ValueState<BloomFilter<Long>> bfState;

                 @Override
                 public void open(Configuration parameters) throws Exception {
                    countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState", Long.class));

                    bfState = getRuntimeContext()
                            .getState(new ValueStateDescriptor<BloomFilter<Long>>("bfState", TypeInformation.of(new TypeHint<BloomFilter<Long>>() {})
                                    )
                            );

                 }

                 @Override
                 public void process(String key,
                                     Context context,
                                     Iterable<UserBehavior> elements, Collector<String> out) throws Exception {
                    countState.update(0L);

                    // 在状态中初始化一个布隆过滤器
                    // 参数1: 漏斗, 存储的类型
                    // 参数2: 期望插入的元素总个数
                    // 参数3: 期望的误判率(假阳性率)
                    BloomFilter<Long> bf = BloomFilter.create(Funnels.longFunnel(), 1000000, 0.001);
                    bfState.update(bf);

                    for (UserBehavior behavior : elements) {
                       // 查布隆
                       if (!bfState.value().mightContain(behavior.getUserId())) {
                          // 不存在 计数+1
                          countState.update(countState.value() + 1L);
                          // 记录这个用户di, 表示来过
                          bfState.value().put(behavior.getUserId());
                       }
                    }
                    out.collect("窗口: " + context.window() + " 的uv是: " + countState.value());
                 }
              })
              .print();
      env.execute();
   }
}
