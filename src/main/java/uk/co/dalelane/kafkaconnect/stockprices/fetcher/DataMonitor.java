package uk.co.dalelane.kafkaconnect.stockprices.fetcher;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Timer;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.co.dalelane.kafkaconnect.stockprices.StockPriceConfig;
import uk.co.dalelane.kafkaconnect.stockprices.data.SourceRecordFactory;
import uk.co.dalelane.kafkaconnect.stockprices.data.StockUnitData;

public class DataMonitor {

    private static Logger log = LoggerFactory.getLogger(DataMonitor.class);

    private boolean isRunning;

    private Timer fetcherTimer;
    private final FetcherTask fetcherTask;
    private final StockUnitCache stockData;
    private final SourceRecordFactory recordFactory;
    
    private final int delayHours;
    private final ZoneOffset zoneOffset;
    
    private static final int ONE_DAY_MS = 86400000;
    
    
    public DataMonitor(StockPriceConfig config, long startTimestamp) {
        log.info("Creating monitor for " + config.getStockSymbol() + 
                " to topic " + config.getTopic() + 
                " from " + startTimestamp);
        
        delayHours = config.getTimeDelayHours();
        zoneOffset = config.getTimeZoneAsOffset();
        
        isRunning = false;
        
        recordFactory = new SourceRecordFactory(config);
        stockData = new StockUnitCache(startTimestamp, zoneOffset);        
        fetcherTask = new FetcherTask(stockData, config);
    }
    

    public synchronized void start() {
        log.info("Starting monitor");        

        if (isRunning == false) {
            fetcherTimer = new Timer();
            fetcherTimer.scheduleAtFixedRate(fetcherTask, 0, ONE_DAY_MS);

            isRunning = true;
        }
    }
            
    public synchronized void stop() {
        log.info("Stopping monitor");
        
        if (isRunning) {
            fetcherTimer.cancel();
            
            isRunning = false;
        }
    }
    
    
    public List<SourceRecord> getRecords() {
        List<StockUnitData> stockRecords = stockData.getStockUnitData(getDelayedDate());
        return stockRecords.stream()
                .map(s -> recordFactory.createSourceRecord(s))
                .collect(Collectors.toList());
    }
    
    
    private long getDelayedDate() {
        return LocalDateTime.now().minusHours(delayHours).toEpochSecond(zoneOffset);
    }
}
