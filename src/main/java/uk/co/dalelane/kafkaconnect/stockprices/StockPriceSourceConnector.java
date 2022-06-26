package uk.co.dalelane.kafkaconnect.stockprices;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Connect source connector that fetches stock price data 
 *  from the Alpha Vantage API.
 */
public class StockPriceSourceConnector extends SourceConnector {

    private final Logger log = LoggerFactory.getLogger(StockPriceSourceConnector.class);

    private StockPriceConfig config;

    
    @Override
    public ConfigDef config() {
        return StockPriceConfig.configDef();
    }

    
    @Override
    public Class<? extends Task> taskClass() {
        return StockPriceSourceTask.class;
    }

    
    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config validatedConfigs = super.validate(connectorConfigs);

        boolean missingApiKey = true;
        boolean missingStockName = true;
        boolean missingForexFromName = true;
        boolean missingForexToName = true;
        boolean missingTopicName = true;

        for (ConfigValue configValue : validatedConfigs.configValues()) {
            if (configValue.name().equals(StockPriceConfig.API_KEY_PARAM_CONFIG)) {
                missingApiKey = false;
            }
            else if (configValue.name().equals(StockPriceConfig.STOCK_SYMBOL_PARAM_CONFIG)) {
                missingStockName = false;
            }
            else if (configValue.name().equals(StockPriceConfig.FOREX_FROM_SYMBOL_PARAM_CONFIG)) {
                missingForexFromName = false;
            }
            else if (configValue.name().equals(StockPriceConfig.FOREX_TO_SYMBOL_PARAM_CONFIG)) {
                missingForexToName = false;
            }
            else if (configValue.name().equals(StockPriceConfig.TOPIC_NAME_PARAM_CONFIG)) {
                missingTopicName = false;
            }
        }
        if (missingApiKey) {
            throw new ConnectException("API key for Alpha Vantage is required (alpha.vantage.api.key)");
        }
        if (missingStockName && missingForexFromName && missingForexToName) {
            throw new ConnectException("Stock symbol is required (stock.symbol)");
        }
        if (missingTopicName) {
            throw new ConnectException("Topic name is required (topic)");
        }
        // TODO: Add checks for forex names
        
        return validatedConfigs;
    }
    
    
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            log.warn("Only one task is supported. Ignoring tasks.max which is set to {}", maxTasks);
        }
        
        List<Map<String, String>> taskConfigs = new ArrayList<>(1);
        taskConfigs.add(config.getTaskConfig());        
        return taskConfigs;
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting connector {}", props);
        config = new StockPriceConfig(props);
    }

    @Override
    public void stop() {
        log.info("Stopping connector");
    }
    
    @Override
    public String version() {
        return "0.0.3";
    }
}
