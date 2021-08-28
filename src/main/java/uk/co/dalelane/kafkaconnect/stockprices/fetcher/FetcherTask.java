package uk.co.dalelane.kafkaconnect.stockprices.fetcher;

import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.crazzyghost.alphavantage.AlphaVantage;
import com.crazzyghost.alphavantage.Config;
import com.crazzyghost.alphavantage.parameters.Interval;
import com.crazzyghost.alphavantage.parameters.OutputSize;
import com.crazzyghost.alphavantage.timeseries.response.TimeSeriesResponse;

import uk.co.dalelane.kafkaconnect.stockprices.StockPriceConfig;

public class FetcherTask extends TimerTask {

    private static Logger log = LoggerFactory.getLogger(FetcherTask.class);
    
    private final StockUnitCache dataCache;
    private final StockPriceConfig connectorConfig;
    
    public FetcherTask(StockUnitCache cache, StockPriceConfig config) {
        log.info("Initializing alphavantage API");
        Config cfg = Config.builder()
                .key(config.getApiKey())
                .timeOut(30)
                .build();
        AlphaVantage.api().init(cfg);
        
        dataCache = cache;
        connectorConfig = config;
    }
    
    @Override
    public void run() {
        log.info("fetching data");
        
        TimeSeriesResponse response = AlphaVantage.api()
                .timeSeries()
                .intraday()
                .forSymbol(connectorConfig.getStockSymbol())
                .interval(Interval.ONE_MIN)
                .outputSize(OutputSize.FULL)
                .fetchSync();
        
        dataCache.addStockUnits(response.getStockUnits());
    }

}
