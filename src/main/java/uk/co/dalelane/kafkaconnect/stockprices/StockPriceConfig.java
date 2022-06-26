package uk.co.dalelane.kafkaconnect.stockprices;

import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import com.crazzyghost.alphavantage.parameters.Interval;

public class StockPriceConfig extends AbstractConfig {

    public static final String TOPIC_NAME_PARAM_CONFIG = "topic";
    public static final String TOPIC_NAME_PARAM_DOC = "Topic to produce to";
    
    public static final String API_KEY_PARAM_CONFIG = "alpha.vantage.api.key";
    public static final String API_KEY_PARAM_DOC = "API key for Alpha Vantage (https://www.alphavantage.co)";

    public static final String STOCK_SYMBOL_PARAM_CONFIG = "stock.symbol";
    public static final String STOCK_SYMBOL_PARAM_DOC = "Stock to get data for (e.g. 'IBM')";
    private static final String STOCK_SYMBOL_PARAM_DEFAULT = "";
    
    public static final String FOREX_FROM_SYMBOL_PARAM_CONFIG = "forex.from.symbol";
    public static final String FOREX_FROM_SYMBOL_PARAM_DOC = "Forex source currency to get data for (e.g. 'GBP')";
    private static final String FOREX_FROM_SYMBOL_PARAM_DEFAULT = "";

    public static final String FOREX_TO_SYMBOL_PARAM_CONFIG = "forex.to.symbol";
    public static final String FOREX_TO_SYMBOL_PARAM_DOC = "Forex destination currency to get data for (e.g. 'USD')";
    private static final String FOREX_TO_SYMBOL_PARAM_DEFAULT = "";
    
    public static final String TIME_DELAY_PARAM_CONFIG = "delay.hours";
    public static final String TIME_DELAY_PARAM_DOC = "How long to wait before delivering a stock price update - defaults to 168 (one week)";
    private static final int TIME_DELAY_PARAM_DEFAULT = 168;

    public static final String TIMEZONE_PARAM_CONFIG = "timezone.offset.hours";
    public static final String TIMEZONE_PARAM_DOC = "Timezone that stock price timestamps are reported in - defaults to -5 (US/Eastern)";
    private static final int TIMEZONE_PARAM_DEFAULT = -5;

    public static final String EVENT_EMIT_INTERVAL_PARAM_CONFIG = "event.emit.interval";
    public static final String EVENT_EMIT_INTERVAL_PARAM_DOC = "Event interval - defaults to 1min (valid values are 1min,5min,15min,30min,60min) ";
    private static final String EVENT_EMIT_INTERVAL_PARAM_DEFAULT = "1min";    
    
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
                STOCK_SYMBOL_PARAM_DEFAULT,
                Importance.HIGH,
                STOCK_SYMBOL_PARAM_DOC)
            .define(
                FOREX_FROM_SYMBOL_PARAM_CONFIG, 
                Type.STRING, 
                FOREX_FROM_SYMBOL_PARAM_DEFAULT,
                Importance.HIGH,
                FOREX_FROM_SYMBOL_PARAM_DOC)
            .define(
                FOREX_TO_SYMBOL_PARAM_CONFIG, 
                Type.STRING, 
                FOREX_TO_SYMBOL_PARAM_DEFAULT,
                Importance.HIGH,
                FOREX_FROM_SYMBOL_PARAM_DOC)
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
                TIMEZONE_PARAM_DOC)
            .define(
                EVENT_EMIT_INTERVAL_PARAM_CONFIG, 
                Type.STRING, 
                EVENT_EMIT_INTERVAL_PARAM_DEFAULT,
                Importance.LOW,
                EVENT_EMIT_INTERVAL_PARAM_DOC);
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
            StockPriceConfig.FOREX_FROM_SYMBOL_PARAM_CONFIG, 
            getForexFromSymbol());
        taskConfig.put(
            StockPriceConfig.FOREX_TO_SYMBOL_PARAM_CONFIG, 
            getForexToSymbol());
        taskConfig.put(
            StockPriceConfig.TIME_DELAY_PARAM_CONFIG, 
            getTimeDelayAsString());
        taskConfig.put(
            StockPriceConfig.EVENT_EMIT_INTERVAL_PARAM_CONFIG, 
            getEventEmitInterval());

        return taskConfig;
    }
    
    public String getTopic() {
        return this.getString(TOPIC_NAME_PARAM_CONFIG);
    }
    
    public String getStockSymbol() {
        return this.getString(STOCK_SYMBOL_PARAM_CONFIG);
    }
    
    public String getForexFromSymbol() {
        return this.getString(FOREX_FROM_SYMBOL_PARAM_CONFIG);
    }
    
    public String getForexToSymbol() {
        return this.getString(FOREX_TO_SYMBOL_PARAM_CONFIG);
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
    
    public String getEventEmitInterval() {
        return this.getString(EVENT_EMIT_INTERVAL_PARAM_CONFIG);
    }

    public Interval getEventEmitIntervalAsEnum() {
        // This doesn't work due to the enum being made up of strings
        //Interval emitInterval = Interval.valueOf(this.getString(EVENT_EMIT_INTERVAL_PARAM_CONFIG));

        String eventEmitInterval = getEventEmitInterval();
        Interval emitInterval = Interval.ONE_MIN;
        if ( eventEmitInterval.equals("5min") ) emitInterval = Interval.FIVE_MIN;
        else if ( eventEmitInterval.equals("15min") ) emitInterval = Interval.FIFTEEN_MIN;
        else if ( eventEmitInterval.equals("30min") ) emitInterval = Interval.THIRTY_MIN;
        else if ( eventEmitInterval.equals("60min") ) emitInterval = Interval.SIXTY_MIN;

        return emitInterval;
    }
    
}
