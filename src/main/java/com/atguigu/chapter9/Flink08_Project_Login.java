package com.atguigu.chapter9;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * @author coderhyh
 * @create 2022-04-02 10:54
 */
class Flink08_Project_Login {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        KeyedStream<LoginEvent, Long> stream = env
                .readTextFile("input/LoginLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new LoginEvent(Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.parseLong(data[3]) * 1000L);
                })
                // 按照用户id分组
                .keyBy(LoginEvent::getUserId);


        //定义模式:2s内，连续两次登录失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getEventTime());
                    }
                })
                .times(2)
                .consecutive()
                .within(Time.milliseconds(2001));

        PatternStream<LoginEvent> ps = CEP.pattern(stream, pattern);

        ps.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                Long userId = map.get("fail").get(0).getUserId();
                return "用户：" + userId + "正在恶意登录";
            }
        }).print();


        //启动执行环境
        env.execute();
    }
}
