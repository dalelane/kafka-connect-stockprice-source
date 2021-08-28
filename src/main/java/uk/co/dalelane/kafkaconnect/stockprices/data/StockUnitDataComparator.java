package uk.co.dalelane.kafkaconnect.stockprices.data;

import java.util.Comparator;

public class StockUnitDataComparator implements Comparator<StockUnitData> {

    @Override
    public int compare(StockUnitData o1, StockUnitData o2) {
        return o1.getTimestampAsInt() - o2.getTimestampAsInt();
    }
}
