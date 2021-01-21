package org.example.flink.example01;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.File;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TextSourceFunction extends RichSourceFunction<String> implements CheckpointedFunction {

    private String path;
    private Scanner scanner;
    private int lineCount;
    private ListState<Integer> lineCountState;
    private boolean restore;

    public TextSourceFunction(String path) {
        this.path = path;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        scanner = new Scanner(new File(path));
        restore = false;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int i = 0;
        while (scanner.hasNextLine()) {
            String s = scanner.nextLine();
            if (i++ < lineCount) {
                continue;
            }
            System.out.println("................." + i);
            ctx.collect(s);
            TimeUnit.SECONDS.sleep(1);
            lineCount = i;
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        lineCountState.clear();
        System.out.println( "最后一次拿到 line state数据：" + lineCount );
        lineCountState.add(lineCount);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        lineCountState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("lineCount", Integer.class));
        if (context.isRestored()) {
            restore = true;
            Iterable<Integer> longs = lineCountState.get();
            longs.forEach(i -> {
                System.out.println("chk数据：" + i);
                lineCount = i;
            });
        }
    }
}
