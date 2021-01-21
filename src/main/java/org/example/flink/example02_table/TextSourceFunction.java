package org.example.flink.example02_table;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.File;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class TextSourceFunction extends RichSourceFunction<String> {

    private String path;
    private Scanner scanner;

    public TextSourceFunction(String path) {
        this.path = path;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        scanner = new Scanner(new File(path));
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (scanner.hasNextLine()) {
            String s = scanner.nextLine();
            ctx.collect(s);
        }
    }

    @Override
    public void cancel() {

    }
}
