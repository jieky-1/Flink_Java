import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

// 水位线测试
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
                // 丢失类型，通过returns来明确数据类型
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        // 单位是毫秒
                        System.out.println(new Timestamp(Long.parseLong(arr[1]) * 1000));
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000);
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
                )
                .keyBy(r -> r.f0)
                // 水位线到达定时器的时候，触发定时器计算
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        // 水位线的计算是基于前一个元素的事件时间计算的，而不是当前元素的事件事件计算的
                        out.collect("当前的水位线是：" + new Timestamp(ctx.timerService().currentWatermark()));
                        ctx.timerService().registerEventTimeTimer(value.f1 + 5000L);
                        out.collect("注册了一个时间戳是：" + new Timestamp(value.f1 + 5000L) + " 的定时器");
                    }

                    // 触发定时器的时候，不会执行processElement
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect(new Timestamp(timestamp) + "定时器触发了！");
                    }
                })
                .print();

        env.execute();
    }
}
