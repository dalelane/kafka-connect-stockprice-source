package uk.co.dalelane.kafkaconnect.stockprices.data;

import java.util.Comparator;

public class MarketUnitDataComparator implements Comparator<MarketUnitData> {

    @Override
    public int compare(MarketUnitData o1, MarketUnitData o2) {
        return o1.getTimestampAsInt() - o2.getTimestampAsInt();
    }
}
