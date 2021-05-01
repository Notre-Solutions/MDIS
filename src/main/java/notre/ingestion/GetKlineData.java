package notre.ingestion;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.binance.BinanceExchange;
import org.knowm.xchange.binance.dto.marketdata.BinanceKline;
import org.knowm.xchange.binance.dto.marketdata.BinanceTicker24h;
import org.knowm.xchange.binance.dto.marketdata.KlineInterval;
import org.knowm.xchange.binance.service.BinanceMarketDataService;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.examples.binance.BinanceDemoUtils;
import org.knowm.xchange.service.marketdata.MarketDataService;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class GetKlineData {
    ChronicleQueueLauncher chronicleLauncher = new ChronicleQueueLauncher();
    Chronicle chronicle = chronicleLauncher.buildIndexedChronicleQueue();
    ExcerptAppender appender = chronicle.createAppender();

    public GetKlineData() throws IOException {
    }

    public static void main(String[] args) throws IOException {

        Exchange exchange = BinanceDemoUtils.createExchange();
        GetKlineData m = new GetKlineData();
        BinanceMarketDataService binanceMarketDataService = (BinanceMarketDataService) exchange.getMarketDataService();

        /* create a data service from the exchange */
        MarketDataService marketDataService = exchange.getMarketDataService();
        List<BinanceKline> klines = binanceMarketDataService.klines(CurrencyPair.BTC_USDT, KlineInterval.m1,9500,(Long) 1617235200000L, (long) 1619800573000L);
        for(BinanceKline kline : klines){
            m.appender.startExcerpt();
            m.appender.writeUTF(kline.getCurrencyPair().toString());
            m.appender.writeUTF(kline.getInterval().toString());
            m.appender.writeLong(kline.getOpenTime());
            m.appender.writeLong(kline.getCloseTime());
            m.appender.writeDouble(kline.getOpenPrice().doubleValue());
            m.appender.writeDouble(kline.getHighPrice().doubleValue());
            m.appender.writeDouble(kline.getLowPrice().doubleValue());
            m.appender.writeDouble(kline.getClosePrice().doubleValue());
            m.appender.writeDouble(kline.getVolume().doubleValue());
            m.appender.writeDouble(kline.getQuoteAssetVolume().doubleValue());
            m.appender.writeLong(kline.getNumberOfTrades());
            m.appender.writeDouble(kline.getTakerBuyBaseAssetVolume().doubleValue());
            m.appender.writeDouble(kline.getTakerBuyQuoteAssetVolume().doubleValue());
            m.appender.finish();
        }

//        generic(exchange, marketDataService);
//        raw((BinanceExchange) exchange, (BinanceMarketDataService) marketDataService);
        // rawAll((BinanceExchange) exchange, (BinanceMarketDataService) marketDataService);
    }

    public static void generic(Exchange exchange, MarketDataService marketDataService)
            throws IOException {}

    public static void raw(BinanceExchange exchange, BinanceMarketDataService marketDataService)
            throws IOException {

        List<BinanceTicker24h> tickers = new ArrayList<>();
        for (CurrencyPair cp : exchange.getExchangeMetaData().getCurrencyPairs().keySet()) {
            if (cp.counter == Currency.USDT) {
                tickers.add(marketDataService.ticker24h(cp));
            }
        }

        Collections.sort(
                tickers,
                new Comparator<BinanceTicker24h>() {
                    @Override
                    public int compare(BinanceTicker24h t1, BinanceTicker24h t2) {
                        return t2.getPriceChangePercent().compareTo(t1.getPriceChangePercent());
                    }
                });

        tickers.stream()
                .forEach(
                        t -> {
                            System.out.println(
                                    t.getCurrencyPair()
                                            + " => "
                                            + String.format("%+.2f%%", t.getPriceChangePercent()));
                        });
        System.out.println("raw out end");
    }

    public static void rawAll(BinanceExchange exchange, BinanceMarketDataService marketDataService)
            throws IOException {

        List<BinanceTicker24h> tickers = new ArrayList<>();
        tickers.addAll(marketDataService.ticker24h());
        Collections.sort(
                tickers,
                new Comparator<BinanceTicker24h>() {
                    @Override
                    public int compare(BinanceTicker24h t1, BinanceTicker24h t2) {
                        return t2.getPriceChangePercent().compareTo(t1.getPriceChangePercent());
                    }
                });

        tickers.stream()
                .forEach(
                        t -> {
                            System.out.println(
                                    t.getSymbol() + " => " + String.format("%+.2f%%", t.getLastPrice()));
                        });
    }

}
