package org.example.flink.example01;

import javax.annotation.Nonnull;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Quote {
    private int price;
    private String code;
    private long ts;
    static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    public Quote() {
    }

    public static Quote fromString(String quoteStr) throws ParseException {
        String[] data = quoteStr.split(",");
        return new Quote(data[0], data[1], dateFormat.parse(data[2]).getTime());
    }

    public Quote(@Nonnull String price, String name, long ts) {
        this.price = Integer.parseInt(price);
        this.code = name;
        this.ts = ts;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "\"Quote\":{" + "\"price\":" + price + "," + "\"code\":\"" + code + "\"" + "," + "\"ts\":" + ts + "}";
    }

}
