package uk.co.dalelane.kafkaconnect.stockprices;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.co.dalelane.kafkaconnect.stockprices.data.StockRecordFactory;
import uk.co.dalelane.kafkaconnect.stockprices.data.ForexRecordFactory;
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
        List<SourceRecord> retval = sourceData.getRecords();
        return retval;
    }

    
    @Override
    public String version() {
        return "0.0.3";
    }
    
    
    
    private long getPersistedOffset(StockPriceConfig config) {
        long offsetTimestamp = 0;
        Map<String, Object> persistedOffsetInfo = null;

        
        if (context != null && context.offsetStorageReader() != null) {
            if ( !config.getStockSymbol().isEmpty() )
            {
                Map<String, Object> sourcePartition = StockRecordFactory.createSourcePartition(config.getStockSymbol());
                persistedOffsetInfo = context.offsetStorageReader().offset(sourcePartition);
            }
            else
            {
                Map<String, Object> sourcePartition = ForexRecordFactory.createSourcePartition(config.getForexFromSymbol()+config.getForexFromSymbol());
                persistedOffsetInfo = context.offsetStorageReader().offset(sourcePartition);
            }
        }
        if (persistedOffsetInfo != null) {
            Object lastOffsetTimestamp = null;
            if ( !config.getStockSymbol().isEmpty() )
                lastOffsetTimestamp = persistedOffsetInfo.get(StockRecordFactory.SOURCE_OFFSET);
            else
                lastOffsetTimestamp = persistedOffsetInfo.get(ForexRecordFactory.SOURCE_OFFSET);

            if (lastOffsetTimestamp != null) {
                offsetTimestamp = (Long) lastOffsetTimestamp;
            }
        }
        return offsetTimestamp;
    }
}
