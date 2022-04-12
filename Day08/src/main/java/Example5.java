import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

// 使用状态机来实现检测连续三次登录失败
public class Example5 {
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

        stream
                .keyBy(r -> r.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    // 状态机
                    private HashMap<Tuple2<String, String>, String> stateMachine = new HashMap<>();
                    // 当前状态
                    private ValueState<String> currentState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 状态转移矩阵
                        // key：(状态，接收到事件的类型)
                        // value：将要跳转到的状态
                        stateMachine.put(Tuple2.of("INITIAL", "success"), "SUCCESS");
                        stateMachine.put(Tuple2.of("INITIAL", "fail"), "S1");
                        stateMachine.put(Tuple2.of("S1", "fail"), "S2");
                        stateMachine.put(Tuple2.of("S2", "fail"), "FAIL");
                        stateMachine.put(Tuple2.of("S1", "success"), "SUCCESS");
                        stateMachine.put(Tuple2.of("S2", "success"), "SUCCESS");

                        currentState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("current-state", Types.STRING)
                        );
                    }

                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        if (currentState.value() == null) {
                            currentState.update("INITIAL");
                        }

                        // 计算将要跳转到的状态
                        String nextState = stateMachine.get(Tuple2.of(currentState.value(), value.eventType));

                        if (nextState.equals("FAIL")) {
                            out.collect("用户" + value.user + "连续三次登录失败了");
                            // 不能重置到初始状态，否则会把前两个fail信息丢失掉
                            currentState.update("S2");
                        } else if (nextState.equals("SUCCESS")) {
                            currentState.clear();
                        } else {
                            currentState.update(nextState);
                        }
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
