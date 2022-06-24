package uk.co.dalelane.kafkaconnect.stockprices.fetcher;

import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.crazzyghost.alphavantage.AlphaVantage;
import com.crazzyghost.alphavantage.Config;
import com.crazzyghost.alphavantage.parameters.Interval;
import com.crazzyghost.alphavantage.parameters.OutputSize;
import com.crazzyghost.alphavantage.forex.response.ForexResponse;
import com.crazzyghost.alphavantage.timeseries.response.TimeSeriesResponse;

import uk.co.dalelane.kafkaconnect.stockprices.StockPriceConfig;

public class FetcherTask extends TimerTask {

    private static Logger log = LoggerFactory.getLogger(FetcherTask.class);
    
    private final MarketUnitCache dataCache;
    private final StockPriceConfig connectorConfig;
    
    public FetcherTask(MarketUnitCache cache, StockPriceConfig config) {
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
        
        if ( !connectorConfig.getStockSymbol().isEmpty() )
        {
            log.info("fetching stock data for interval "+connectorConfig.getEventEmitIntervalAsEnum().toString());
        
            TimeSeriesResponse response = AlphaVantage.api()
                .timeSeries()
                .intraday()
                .forSymbol(connectorConfig.getStockSymbol())
                .interval(connectorConfig.getEventEmitIntervalAsEnum())
                .outputSize(OutputSize.FULL)
                .fetchSync();

            //log.info("retrieved data: "+response.getStockUnits());

            dataCache.addUnits(response.getStockUnits());
        }
        else
        {
            log.info("fetching forex data for interval "+connectorConfig.getEventEmitIntervalAsEnum().toString());

            ForexResponse response = AlphaVantage.api()
                .forex()
                .intraday()
                .fromSymbol(connectorConfig.getForexFromSymbol())
                .toSymbol(connectorConfig.getForexToSymbol())
                .interval(connectorConfig.getEventEmitIntervalAsEnum())
                .outputSize(OutputSize.FULL)
                .fetchSync();
            
            //log.info("retrieved data: "+response.getForexUnits());

            dataCache.addForexUnits(response.getForexUnits());
        }
    }

}
