import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 使用状态机来实现检测连续三次登录失败
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env
                .fromElements(
                        new Event("order-1", "create", 1000L),
                        new Event("order-2", "create", 2000L),
                        new Event("order-1", "pay", 3000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("create")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .next("pay")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                .within(Time.seconds(5)); // 要求两个事件在5s之内发生

        PatternStream<Event> patternStream = CEP.pattern(stream.keyBy(r -> r.orderId), pattern);

        SingleOutputStreamOperator<String> result = patternStream
                .flatSelect(
                        new OutputTag<String>("timeout") {
                        }, // 超时事件将发送到侧输出流
                        new PatternFlatTimeoutFunction<Event, String>() {
                            @Override
                            public void timeout(Map<String, List<Event>> map, long l, Collector<String> collector) throws Exception {
                                Event create = map.get("create").get(0);
                                // 发送到侧输出流中去
                                collector.collect("订单：" + create.orderId + " 超时了");
                            }
                        },
                        new PatternFlatSelectFunction<Event, String>() {
                            @Override
                            public void flatSelect(Map<String, List<Event>> map, Collector<String> collector) throws Exception {
                                Event pay = map.get("pay").get(0);
                                collector.collect("订单 " + pay.orderId + " 已支付");
                            }
                        }
                );

        result.print();
        result.getSideOutput(new OutputTag<String>("timeout"){}).print();


        env.execute();
    }
    public static class Event {
        public String orderId;
        public String eventType;
        public Long timestamp;

        public Event() {
        }

        public Event(String orderId, String eventType, Long timestamp) {
            this.orderId = orderId;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }
    }
}
