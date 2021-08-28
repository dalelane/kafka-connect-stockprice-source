package uk.co.dalelane.kafkaconnect.stockprices;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.co.dalelane.kafkaconnect.stockprices.data.SourceRecordFactory;
import uk.co.dalelane.kafkaconnect.stockprices.fetcher.DataMonitor;

public class StockPriceSourceTask extends SourceTask {

    private static Logger log = LoggerFactory.getLogger(StockPriceSourceTask.class);

    private DataMonitor sourceData = null;

    @Override
    public void start(Map<String, String> properties) {
        log.info("Starting task {}", properties);
        
        if (sourceData == null) {
            StockPriceConfig config = new StockPriceConfig(properties);            
            sourceData = new DataMonitor(config, getPersistedOffset(config));
        }
        
        sourceData.start();
    }

    
    @Override
    public void stop() {
        log.info("Stopping task");
        if (sourceData != null) {
            sourceData.stop();
        }
    }
    
    
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return sourceData.getRecords();
    }

    
    @Override
    public String version() {
        return "0.0.2";
    }
    
    
    
    private long getPersistedOffset(StockPriceConfig config) {
        long offsetTimestamp = 0;
        Map<String, Object> persistedOffsetInfo = null;
        if (context != null && context.offsetStorageReader() != null) {
            String stock = config.getStockSymbol();           
            Map<String, Object> sourcePartition = SourceRecordFactory.createSourcePartition(stock);
            persistedOffsetInfo = context.offsetStorageReader().offset(sourcePartition);
        }
        if (persistedOffsetInfo != null) {
            Object lastOffsetTimestamp = persistedOffsetInfo.get(SourceRecordFactory.SOURCE_OFFSET);
            if (lastOffsetTimestamp != null) {
                offsetTimestamp = (Long) lastOffsetTimestamp;
            }
        }
        return offsetTimestamp;
    }
}
