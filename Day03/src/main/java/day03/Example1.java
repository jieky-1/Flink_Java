package day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 富函数
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);

        env
                .fromElements(1,2,3)
                .map(new RichMapFunction<Integer, Integer>() {
                    // open() 函数通常用来做一些只需要做一次即可的初始化工作
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("生命周期开始");
                        // getRuntimeContext() 方法提供了函数的 RuntimeContext 的一些信息
                        System.out.println("当前子任务的索引是：" + this.getRuntimeContext().getIndexOfThisSubtask());
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        System.out.println("并行度：" + this.getRuntimeContext().getNumberOfParallelSubtasks());
                        return value * value;
                    }
                    // close() 方法是生命周期中的最后一个调用的方法，通常用来做一些清理工作。
                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("生命周期结束");
                    }
                })
                .print();

        env.execute();
    }
}
