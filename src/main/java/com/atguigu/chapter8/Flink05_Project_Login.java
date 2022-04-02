package com.atguigu.chapter8;

import com.atguigu.bean.LoginEvent;
import com.atguigu.util.AtguiguUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-02 10:54
 */
class Flink05_Project_Login {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env
                .readTextFile("input/LoginLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new LoginEvent(Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.parseLong(data[3]) * 1000L);
                })
                // 按照用户id分组
                .keyBy(LoginEvent::getUserId)
                .countWindow(2, 1)
                .process(new ProcessWindowFunction<LoginEvent, String, Long, GlobalWindow>() {
                    @Override
                    public void process(Long userId,
                                        ProcessWindowFunction<LoginEvent, String, Long, GlobalWindow>.Context context,
                                        Iterable<LoginEvent> elements,
                                        Collector<String> out) throws Exception {

                        List<LoginEvent> list = AtguiguUtil.toList(elements);
                        if (list.size() == 2) {
                            LoginEvent event1 = list.get(0);
                            LoginEvent event2 = list.get(1);

                            String eventType1 = event1.getEventType();
                            String eventType2 = event2.getEventType();


                            Long eventTime1 = event1.getEventTime();
                            Long eventTime2 = event2.getEventTime();

                            if ("fail".equals(eventType1) && "fail".equals(eventType2) && Math.abs(eventTime1 - eventTime2) < 2000) {
                                out.collect("用户：" + userId + "在进行恶意登录");
                            }
                        }

                    }
                }).print();


        //启动执行环境
        env.execute();
    }
}
