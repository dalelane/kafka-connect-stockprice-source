package uk.co.dalelane.kafkaconnect.stockprices.data;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import com.crazzyghost.alphavantage.timeseries.response.StockUnit;

public class StockUnitData {

    private double open;
    private double high;
    private double low;
    private double close;
    private long volume;
    private long timestamp;
    private String datetime;
    
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    
    public StockUnitData(StockUnit data, ZoneOffset offset) {
        open = data.getOpen();
        high = data.getHigh();
        low = data.getLow();
        close = data.getClose();
        volume = data.getVolume();
        datetime = data.getDate();
        timestamp = parseDateString(data.getDate(), offset);
    }

    
    private static final long parseDateString(String dateStr, ZoneOffset offset) {
        return LocalDateTime.parse(dateStr, FORMATTER).toEpochSecond(offset);
    }
    
    public double getOpen() {
        return open;
    }

    public double getHigh() {
        return high;
    }

    public double getLow() {
        return low;
    }

    public double getClose() {
        return close;
    }

    public long getVolume() {
        return volume;
    }

    public long getTimestamp() {
        return timestamp;
    }
    
    public int getTimestampAsInt() {
        return Long.valueOf(timestamp).intValue();
    }
    
    public String getDateTime() {
        return datetime;
    }
        
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof StockUnitData) {
            return ((StockUnitData)o).getTimestamp() == getTimestamp();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getTimestampAsInt();
    }

    @Override
    public String toString() {
        return "{ " + 
            "\"open\" : " + open + ", " + 
            "\"high\" : " + high + ", " + 
            "\"open\" : " + open + ", " + 
            "\"low\" : " + low + ", " + 
            "\"close\" : " + close + ", " + 
            "\"volume\" : " + volume + ", " + 
            "\"timestamp\" : " + timestamp + ", " + 
            "\"datetime\" : " + datetime + 
            " }";
    }
}