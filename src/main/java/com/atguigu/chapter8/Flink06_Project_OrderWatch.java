package com.atguigu.chapter8;

import com.atguigu.bean.OrderEvent;
import com.atguigu.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-02 11:24
 */
class Flink06_Project_OrderWatch {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 创建WatermarkStrategy
        WatermarkStrategy<OrderEvent> wms = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime();
                    }
                });
        env
                .readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(datas[0]),
                            datas[1],
                            datas[2],
                            Long.parseLong(datas[3]) * 1000);

                })
                .assignTimestampsAndWatermarks(wms)
                .keyBy(OrderEvent::getOrderId)
                .window(EventTimeSessionWindows.withGap(Time.minutes(45)))
                .process(new ProcessWindowFunction<OrderEvent, String, Long, TimeWindow>() {

                    private ValueState<OrderEvent> createState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createState", OrderEvent.class));
                    }

                    @Override
                    public void process(Long orderId,
                                        ProcessWindowFunction<OrderEvent, String, Long, TimeWindow>.Context context,
                                        Iterable<OrderEvent> elements,
                                        Collector<String> out) throws Exception {

                        List<OrderEvent> list = AtguiguUtil.toList(elements);

                        if (list.size() == 2) {
                            out.collect("订单:" + orderId + "正常支付...");
                        } else {

                            OrderEvent orderEvent = list.get(0);
                            String eventType = orderEvent.getEventType();
                            if ("create".equals(eventType)) {
                                createState.update(orderEvent);
                            } else {
                                if (createState.value() != null) {
                                    out.collect("订单:" + orderId + "超时支付，请检查是否有漏洞...");
                                } else {
                                    out.collect("订单:" + orderId + "只有支付没有创建，请检查是否有漏洞...");
                                }
                            }
                        }
                    }

                }).print();


        //启动执行环境
        env.execute();
    }
}
