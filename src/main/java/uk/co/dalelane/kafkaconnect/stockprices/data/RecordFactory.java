package uk.co.dalelane.kafkaconnect.stockprices.data;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import uk.co.dalelane.kafkaconnect.stockprices.StockPriceConfig;

public abstract class RecordFactory {
    public static final String SOURCE_OFFSET = "timestamp";
    
    public abstract SourceRecord createSourceRecord(MarketUnitData data);
}
