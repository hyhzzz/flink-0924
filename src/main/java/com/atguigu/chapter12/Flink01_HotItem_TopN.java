package com.atguigu.chapter12;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-05 20:17
 */
class Flink01_HotItem_TopN {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 使用sql从文件读取数据
        tenv.executeSql(
                "create table user_behavior(" +
                        "   user_id bigint, " +
                        "   item_id bigint, " +
                        "   category_id int, " +
                        "   behavior string, " +
                        "   ts bigint, " +
                        "   event_time as to_timestamp(from_unixtime(ts, 'yyyy-MM-dd HH:mm:ss')), " +
                        "   watermark for event_time as  event_time - interval '5' second " +
                        ")with(" +
                        "   'connector'='filesystem', " +
                        "   'path'='input/UserBehavior.csv', " +
                        "   'format'='csv')"
        );

        // 每隔 10m 统计一次最近 1h 的热门商品 top
        // 1. 计算每每个窗口内每个商品的点击量
        //tenv.sqlQuery("select * from user_behavior").execute().print();

        Table t1 = tenv
                .sqlQuery(
                        "select " +
                                "   item_id, " +
                                "   hop_end(event_time, interval '10' minute, interval '1' hour) w_end," +
                                "   count(*) item_count " +
                                "from user_behavior " +
                                "where behavior='pv' " +
                                "group by hop(event_time, interval '10' minute, interval '1' hour), item_id"
                );
        tenv.createTemporaryView("t1", t1);

        // 2. 按照窗口开窗, 对商品点击量进行排名
        Table t2 = tenv.sqlQuery(
                "select " +
                        "   *," +
                        "   row_number() over(partition by w_end order by item_count desc) rk " +
                        "from t1"
        );
        tenv.createTemporaryView("t2", t2);

        // 3. 取 top3
        Table t3 = tenv.sqlQuery(
                "select " +
                        "   item_id, w_end, item_count, rk " +
                        "from t2 " +
                        "where rk<=3"
        );

        // 4. 数据写入到mysql
        // 4.1 创建输出表
        tenv.executeSql("create table hot_item(" +
                "   item_id bigint, " +
                "   w_end timestamp(3), " +
                "   item_count bigint, " +
                "   rk bigint, " +
                "   PRIMARY KEY (w_end, rk) NOT ENFORCED)" +
                "with(" +
                "   'connector' = 'jdbc', " +
                "   'url' = 'jdbc:mysql://hadoop102:3306/flink_sql?useSSL=false', " +
                "   'table-name' = 'hot_item', " +
                "   'username' = 'root', " +
                "   'password' = '123456' " +
                ")");

        // 4.2 写入到输出表
        t3.executeInsert("hot_item");


    }
}
