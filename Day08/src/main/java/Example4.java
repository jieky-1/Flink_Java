import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

// 使用Flink-CEP检测连续3次登录失败
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env
                .fromElements(
                        new Event("user-1", "fail", 1000L),
                        new Event("user-1", "fail", 2000L),
                        new Event("user-1", "fail", 3000L),
                        new Event("user-2", "success", 3000L),
                        new Event("user-1", "fail", 4000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("fail") // 为第一个匹配事件起名字
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .times(3);

        // 在流上匹配模板
        // keyBy后四个fail变成了连续的
        PatternStream<Event> patternStream = CEP.pattern(stream.keyBy(r -> r.user), pattern);

        // 使用select方法将匹配到的事件取出
        patternStream
                .select(new PatternSelectFunction<Event, String>() {
                    @Override
                    public String select(Map<String, List<Event>> map) throws Exception {
                        // Map的key是给事件起的名字
                        // 列表是名字对应的事件所构成的列表
                        Event first = map.get("fail").get(0);
                        Event second = map.get("fail").get(1);
                        Event third = map.get("fail").get(2);
                        String result = "用户：" + first.user + " 在时间：" + first.timestamp + ";" + second.timestamp + ";" +
                                "" + third.timestamp + " 登录失败了！";
                        return result;
                    }
                })
                .print();

        env.execute();
    }

    public static class Event {
        public String user;
        public String eventType;
        public Long timestamp;

        public Event() {
        }

        public Event(String user, String eventType, Long timestamp) {
            this.user = user;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }
    }
}
