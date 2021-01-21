package org.example.flink.example02_table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableShowcase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.addSource(new TextSourceFunction("D:/wuzhsh/java/flink-in-action/src/main/resources/source.txt"));
        SingleOutputStreamOperator<Quote> mappedStream = dataStream.map(Quote::fromString).setParallelism(2);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 基于流创建表
        Table table = tableEnv.fromDataStream(mappedStream);
        // table api
        Table result = table.select("price, code")
                .where("code='000001.sz'");
        DataStream<Row> tableRes = tableEnv.toAppendStream(result, Row.class);
        tableRes.print("table");

        // sql
        tableEnv.createTemporaryView("quote", table);
        String sql = "select * from quote where code='000001.sz'";
        Table sqlTable = tableEnv.sqlQuery(sql);
        DataStream<Quote> sqlRes = tableEnv.toAppendStream(sqlTable, Quote.class);
        sqlRes.print("sql");

        env.execute();

    }
}
