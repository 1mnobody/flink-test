package org.example.flink.example01;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

public class WindowExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        /*
        // checkpoint 测试, flink run -s file:///D:/checkpoint/xxxxxxxx/chk-x  可以让 job 从指定的状态启动（代码中可能要处理状态恢复）
        // 这里需要注意的是，如果job完成了重启策略之后依旧是失败的，那么checkpoint数据会被删除
        env.enableCheckpointing(3000);
        env.setStateBackend(new FsStateBackend("file:///D:/checkpoint/"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));
        DataStream<String> dataStream = env.addSource(new TextSourceFunction("D:/wuzhsh/java/flink-in-action/src/main/resources/source.txt"));
        */
        DataStream<String> dataStream = env.addSource(new TextSourceFunction("D:/wuzhsh/java/flink-in-action/src/main/resources/source2.txt"));
        SingleOutputStreamOperator<Quote> mappedStream = dataStream.map(Quote::fromString).setParallelism(2);
        // TimestampsAndPeriodicWatermarksOperator中覆盖了 processWatermark 方法，只接收 source 下发的 Watermark.MAX_WATERMARK，
        // 当收到此 Watermark 时，直接往下游继续发送该 watermark。
        // 以上是 为什么对于文本流，最后一条数据能够触发窗口的原因，因为source关闭时，会发送 Watermark.MAX_WATERMARK，当 TimestampsAndPeriodicWatermarksOperator 接收到之后，
        // 会发送到下游的窗口处理算子中，触发了窗口的计算。
        DataStream<String> res = mappedStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Quote>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Quote element) {
                        return element.getTs();
                    }
                })
                .setParallelism(1)
                .keyBy(Quote::getCode)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
//                .trigger(new MyTrigger())
                .process(new ProcessWindowFunction<Quote, String, String, TimeWindow>() {
                    ValueState<String> lastData;
                    int i = 0;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                    }

                    @Override
                    public void process(String key, Context context, Iterable<Quote> elements, Collector<String> out) throws Exception {
                        i++;
                        Iterator<Quote> iterator = elements.iterator();
                        if (lastData == null) {
                            lastData = context.windowState().getState(new ValueStateDescriptor<String>("last", String.class));
                        }
//                        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>  : " + lastData.value());
                        iterator.forEachRemaining(e -> {
                            try {
                                lastData.update(key + "-" + e.toString());
                            } catch (IOException ioException) {
                                ioException.printStackTrace();
                            }
                            out.collect(key + "-" + e.toString());
                        });
                        System.out.println(context.window().getEnd() + " >>>  " + context.currentWatermark());
//                        if (i == 6) {
//                            throw new RuntimeException();
//                        }
//                        System.out.println(Thread.currentThread().toString());
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        if (lastData != null) {
                            lastData.clear();
                        }
                    }
                }).setParallelism(1);

        res.print("result").setParallelism(1);

        env.execute();
    }

    static class MyTrigger extends Trigger<Object, TimeWindow> {

        EventTimeTrigger trigger = EventTimeTrigger.create();
        private Object lastObject;

        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            lastObject = element;
            return trigger.onElement(element, timestamp, window, ctx);
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return trigger.onProcessingTime(time, window, ctx);
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            TriggerResult res = trigger.onEventTime(time, window, ctx);
            if (res.isFire()) {
                System.out.println("eeeeeeee: " + lastObject);
            }
            return res;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            trigger.clear(window, ctx);
        }
    }
}
