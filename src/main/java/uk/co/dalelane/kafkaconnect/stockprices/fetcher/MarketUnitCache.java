package uk.co.dalelane.kafkaconnect.stockprices.fetcher;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.crazzyghost.alphavantage.timeseries.response.StockUnit;

import uk.co.dalelane.kafkaconnect.stockprices.data.StockUnitData;
import uk.co.dalelane.kafkaconnect.stockprices.data.StockUnitDataComparator;

public class StockUnitCache {
    
    private final SortedSet<StockUnitData> stockUnitsCache;
    private long offsetTimestamp;
    private ZoneOffset tzOffset;
    
    public StockUnitCache(long timestamp, ZoneOffset offset) {
        stockUnitsCache = new TreeSet<>(new StockUnitDataComparator());
        offsetTimestamp = timestamp;
        tzOffset = offset;
    }
    
    public synchronized void addStockUnits(List<StockUnit> stockunits) {
        for (StockUnit su : stockunits) {
            StockUnitData stockUnitData = new StockUnitData(su, tzOffset);
            if (stockUnitData.getTimestamp() > offsetTimestamp) {
                stockUnitsCache.add(stockUnitData);
            }
        }
    }
    
    public synchronized List<StockUnitData> getStockUnitData(long timestampThreshold) {
        List<StockUnitData> items = new ArrayList<>();
        
        while (stockUnitsCache.isEmpty() == false) {
            StockUnitData nextItem = stockUnitsCache.first();
            
            if (nextItem.getTimestamp() < timestampThreshold) {
                stockUnitsCache.remove(nextItem);
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