package uk.co.dalelane.kafkaconnect.stockprices.data;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import uk.co.dalelane.kafkaconnect.stockprices.StockPriceConfig;

public class StockRecordFactory extends RecordFactory {
    
    public static final String SOURCE_OFFSET = "timestamp";
    public static final String SOURCE_PARTITION = "stock";
   
    private final String topicName;
    private final String stockSymbol;
    
    public StockRecordFactory(StockPriceConfig config) {
        this.topicName = config.getTopic();
        this.stockSymbol = config.getStockSymbol();
    }
    
    private static final Schema SCHEMA = SchemaBuilder.struct().name("stockdata")
            .field("open", Schema.FLOAT64_SCHEMA)
            .field("high", Schema.FLOAT64_SCHEMA)
            .field("low", Schema.FLOAT64_SCHEMA)
            .field("close", Schema.FLOAT64_SCHEMA)
            .field("volume", Schema.INT64_SCHEMA)
            .field("timestamp", Schema.INT64_SCHEMA)
            .field("datetime", Schema.STRING_SCHEMA)
            .build();
    
    private static Struct createStruct(MarketUnitData data) {
        Struct struct =  new Struct(SCHEMA);
        struct.put(SCHEMA.field("open"), data.getOpen());
        struct.put(SCHEMA.field("high"), data.getHigh());
        struct.put(SCHEMA.field("low"), data.getLow());
        struct.put(SCHEMA.field("close"), data.getClose());
        struct.put(SCHEMA.field("volume"), data.getVolume());
        struct.put(SCHEMA.field("timestamp"), data.getTimestamp());
        struct.put(SCHEMA.field("datetime"), data.getDateTime());
        return struct;
    }
    
    
    public SourceRecord createSourceRecord(MarketUnitData data) {
        return new SourceRecord(createSourcePartition(stockSymbol), 
                                createSourceOffset(data), 
                                topicName, 
                                SCHEMA, 
                                createStruct(data));                                
    }
    
    
    private static Map<String, Object> createSourceOffset(MarketUnitData data) {
        return Collections.singletonMap(SOURCE_OFFSET, data.getTimestamp());
    }
    
    public static Map<String, Object> createSourcePartition(String stocksymbol) {
        return Collections.singletonMap(SOURCE_PARTITION, stocksymbol);
    }
}
