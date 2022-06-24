# kafka-connect-stockprice-source

kafka-connect-stockprice-source is a Kafka Connect source connector for importing data from [Alpha Vantage](https://www.alphavantage.co) into [Apache Kafka](https://kafka.apache.org).

## Building the connector

```sh
mvn package
```

## Sample event

Events are produced every minute (during trading hours).

```json
{
    "open"      : 147.288,
    "high"      : 147.37,
    "low"       : 147.12,
    "close"     : 147.35,
    "volume"    : 660905,
    "timestamp" : 1629478800,
    "datetime"  : "2021-08-20 12:00:00"
}
```
For foreign exchange data (FX), data is produced for the currency pair specified.
```json
{
    "open"      : 1.23,
    "high"      : 1.23,
    "low"       : 1.22,
    "close"     : 1.23,
    "timestamp" : 1655757900,
    "datetime"  : "2022-06-20 20:45:00"
}
```

## Configuration


| Name                    | Description                                   | Type    | Default | Example                    |
| ----------------------- | --------------------------------------------- | ------- | ------- | -------------------------- |
| `alpha.vantage.api.key` | API key for Alpha Vantage                     | String  |         | AB123CD4EF5G6HIJ           |
| `stock.symbol`          | Stock symbol to retrieve prices for           | String  |         | IBM                        |
| `topic`                 | Kafka topic to produce stock price data to    | String  |         | TOPIC.IBM                  |
| `delay.hours`           | Number of hours to delay delivering events    | Integer | 168     | 24                         |
| `forex.from.symbol`     | Source currency (if no stock symbol)          | String  |         | GBP                        |
| `forex.to.symbol`       | Destination currency (if no stock symbol)     | String  |         | USD                        |
| `event.emit.interval`   | Event publish interval                        | String  | 1min    | 15min                      |

A sample configuration file called [`sample-connector.properties`](https://github.com/dalelane/kafka-connect-stockprice-source/blob/main/sample-connector.properties) is included.

## Delay

This was written for a quick prototype proof-of-concept based on processing live stock price events, but I wanted something that I could use with a [free API key](https://www.alphavantage.co/support/#api-key).

To enable this, the connector is downloading historical events using [an Alpha Vantage API that returns several days of one-minute interval time-series records for a stock](https://www.alphavantage.co/documentation/#intraday). It then sends individual price events to Kafka time-shifted by `delay.hours`.

For example, if you set `delay.hours` to 24, then it will download the last day of stock price records, and then start sending them to Kafka - each event produced 24 hours after the timestamp in the price record.

This means **the Connector only needs to make one API call a day**, which is easily within the limits for a free API key. But it produces an event every minute (during trading hours) which makes it useful for demos and proof-of-concepts that need (pseudo-)real-time stock price events.

By default, the delay is set to 168 hours (one week) which means events are published on weekdays and not at weekends, which helps make for a more believable data source.
