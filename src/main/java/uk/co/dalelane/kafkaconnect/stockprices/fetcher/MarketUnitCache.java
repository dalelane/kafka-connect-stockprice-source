package uk.co.dalelane.kafkaconnect.stockprices.fetcher;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.crazzyghost.alphavantage.timeseries.response.StockUnit;
import com.crazzyghost.alphavantage.forex.response.ForexUnit;

import uk.co.dalelane.kafkaconnect.stockprices.data.MarketUnitData;
import uk.co.dalelane.kafkaconnect.stockprices.data.MarketUnitDataComparator;

public class MarketUnitCache {
    
    private static Logger log = LoggerFactory.getLogger(MarketUnitCache.class);
    
    private final SortedSet<MarketUnitData> marketUnitsCache;
    private long offsetTimestamp;
    private ZoneOffset tzOffset;
    
    public MarketUnitCache(long timestamp, ZoneOffset offset) {
        marketUnitsCache = new TreeSet<>(new MarketUnitDataComparator());
        offsetTimestamp = timestamp;
        tzOffset = offset;
    }
    
    public synchronized void addUnits(List<StockUnit> stockunits) {
        int numberAdded = 0;
        for (StockUnit su : stockunits) {
            MarketUnitData MarketUnitData = new MarketUnitData(su, tzOffset);

            if (MarketUnitData.getTimestamp() > offsetTimestamp) {
                numberAdded++;
                marketUnitsCache.add(MarketUnitData);
            }
        }
        log.info("Found "+stockunits.size()+" StockUnits and added "+numberAdded+" based on offset timestamp "+offsetTimestamp);
    }
    
    public synchronized void addForexUnits(List<ForexUnit> forexunits) {
        int numberAdded = 0;
        for (ForexUnit su : forexunits) {
            MarketUnitData MarketUnitData = new MarketUnitData(su, tzOffset);

            if (MarketUnitData.getTimestamp() > offsetTimestamp) {
                numberAdded++;
                marketUnitsCache.add(MarketUnitData);
            }
        }
        log.info("Found "+forexunits.size()+" ForexUnits and added "+numberAdded+" based on offset timestamp "+offsetTimestamp);
    }
    
    public synchronized List<MarketUnitData> getMarketUnitData(long timestampThreshold) {
        List<MarketUnitData> items = new ArrayList<>();
        
        while (marketUnitsCache.isEmpty() == false) {
            MarketUnitData nextItem = marketUnitsCache.first();
            
            if (nextItem.getTimestamp() < timestampThreshold) {
                marketUnitsCache.remove(nextItem);
                offsetTimestamp = nextItem.getTimestamp();
                
                items.add(nextItem);
            }
            else {
                break;
            }
        }

        return items;
    }
}