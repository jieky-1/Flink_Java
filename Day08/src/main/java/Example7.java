import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;

// 流 -> 动态表
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 数据源，且设置水位线
        // 第一步，制作 source 输入
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream = env
                .fromElements(
                        Tuple3.of("Mary", "./home", 12 * 60 * 60 * 1000L),
                        Tuple3.of("Bob", "./cart", 12 * 60 * 60 * 1000L),
                        Tuple3.of("Mary", "./prod?id=1", 12 * 60 * 60 * 1000L + 5 * 1000L),
                        Tuple3.of("Liz", "./home", 12 * 60 * 60 * 1000L + 60 * 1000L),
                        Tuple3.of("Bob", "./prod?id=3", 12 * 60 * 60 * 1000L + 90 * 1000L),
                        Tuple3.of("Mary", "./prod?id=7", 12 * 60 * 60 * 1000L + 105 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                })
                );

        // Flink SQL 动态表的持续查询:https://www.modb.pro/db/100541
        // Flink 动态表操作:https://icode.best/i/65708345830357
        // Flink SQL 入门和实战：https://cloud.tencent.com/developer/article/1450865
        // Flink SQL 窗口函数:https://segmentfault.com/a/1190000023296719

        // 第二步，创建上下文环境：
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        // 第三步，将 source 数据注册成表。数据流 -> 动态表
        Table table = tableEnvironment
                .fromDataStream(
                        stream,
                        $("f0").as("user"),
                        $("f1").as("url"),
                        // 使用rowtime方法指定f2字段是事件时间：不然TABLE不知道那个字段是事件时间
                        $("f2").rowtime().as("cTime")
                );
        tableEnvironment.createTemporaryView("click", table);

        // 第四步，核心处理逻辑 SQL 的编写
        Table result = tableEnvironment.sqlQuery("select user,count(user) from click group by user");

        // 第五步，输出结果。动态表 -> 数据流
        tableEnvironment.toChangelogStream(result).print("result");

        env.execute();
    }
}
