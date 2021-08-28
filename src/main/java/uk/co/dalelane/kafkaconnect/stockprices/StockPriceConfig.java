package uk.co.dalelane.kafkaconnect.stockprices;

import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class StockPriceConfig extends AbstractConfig {

    public static final String TOPIC_NAME_PARAM_CONFIG = "topic";
    public static final String TOPIC_NAME_PARAM_DOC = "Topic to produce to";
    
    public static final String API_KEY_PARAM_CONFIG = "alpha.vantage.api.key";
    public static final String API_KEY_PARAM_DOC = "API key for Alpha Vantage (https://www.alphavantage.co)";

    public static final String STOCK_SYMBOL_PARAM_CONFIG = "stock.symbol";
    public static final String STOCK_SYMBOL_PARAM_DOC = "Stock to get data for (e.g. 'IBM')";
    
    public static final String TIME_DELAY_PARAM_CONFIG = "delay.hours";
    public static final String TIME_DELAY_PARAM_DOC = "How long to wait before delivering a stock price update - defaults to 168 (one week)";
    private static final int TIME_DELAY_PARAM_DEFAULT = 168;
    
    public static final String TIMEZONE_PARAM_CONFIG = "timezone.offset.hours";
    public static final String TIMEZONE_PARAM_DOC = "Timezone that stock price timestamps are reported in - defaults to -5 (US/Eastern)";
    private static final int TIMEZONE_PARAM_DEFAULT = -5;
    
    
    public StockPriceConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }
    
    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(
                TOPIC_NAME_PARAM_CONFIG, 
                Type.STRING,
                Importance.HIGH,
                TOPIC_NAME_PARAM_DOC)
            .define(
                API_KEY_PARAM_CONFIG, 
                Type.STRING,
                Importance.HIGH,
                API_KEY_PARAM_DOC)
            .define(
                STOCK_SYMBOL_PARAM_CONFIG, 
                Type.STRING, 
                Importance.HIGH,
                STOCK_SYMBOL_PARAM_DOC)
            .define(
                TIME_DELAY_PARAM_CONFIG, 
                Type.INT,
                TIME_DELAY_PARAM_DEFAULT, 
                Importance.LOW,
                TIME_DELAY_PARAM_DOC)
            .define(
                TIMEZONE_PARAM_CONFIG, 
                Type.INT,
                TIMEZONE_PARAM_DEFAULT, 
                Importance.LOW,
                TIMEZONE_PARAM_DOC);
    }
    
    public Map<String, String> getTaskConfig() {
        Map<String, String> taskConfig = new HashMap<>();

        taskConfig.put(
            StockPriceConfig.TOPIC_NAME_PARAM_CONFIG, 
            getTopic());
        taskConfig.put(
            StockPriceConfig.API_KEY_PARAM_CONFIG, 
            getApiKey());
        taskConfig.put(
            StockPriceConfig.STOCK_SYMBOL_PARAM_CONFIG, 
            getStockSymbol());
        taskConfig.put(
            StockPriceConfig.TIME_DELAY_PARAM_CONFIG, 
            getTimeDelayAsString());
        
        return taskConfig;
    }
    
    public String getTopic() {
        return this.getString(TOPIC_NAME_PARAM_CONFIG);
    }
    
    public String getStockSymbol() {
        return this.getString(STOCK_SYMBOL_PARAM_CONFIG);
    }
    
    public String getApiKey() {
        return this.getString(API_KEY_PARAM_CONFIG);
    }
    
    public Integer getTimeDelayHours() {
        return this.getInt(TIME_DELAY_PARAM_CONFIG);
    }
    public String getTimeDelayAsString() {
        return Integer.toString(getTimeDelayHours());
    }

    public Integer getTimeZoneHours() {
        return this.getInt(TIMEZONE_PARAM_CONFIG);
    }
    public String getTimeZoneHoursAsString() {
        return Integer.toString(getTimeZoneHours());
    }
    public ZoneOffset getTimeZoneAsOffset() {
        return ZoneOffset.ofHours(getTimeZoneHours());
    }
}
