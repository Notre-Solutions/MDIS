package notre.ingestion;

import info.bitrich.xchangestream.binance.BinanceSubscriptionType;
import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import info.bitrich.xchangestream.binance.BinanceStreamingExchange;
import io.reactivex.disposables.Disposable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.knowm.xchange.binance.service.BinanceMarketDataServiceRaw;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.Trade;

public class Main {
    private final static Logger LOG = Logger.getLogger(Main.class.getName());
    private BinanceStreamingExchange exchange = null;
    private List<Disposable> listOfDisposables= new ArrayList<>();

    ChronicleQueueLauncher chronicleLauncher = new ChronicleQueueLauncher();
    Chronicle chronicle = chronicleLauncher.buildIndexedChronicleQueue();
    ExcerptAppender appender = chronicle.createAppender();

    public Main() throws IOException {
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        Main m = new Main();

        Disposable tradesBtc = m.createBinanceConnectionAndSubscribe();
        m.listOfDisposables.add(tradesBtc);
        // We live subscribe a new currency pair to the trades update
        m.listOfDisposables.add(m.addToStream(CurrencyPair.ETH_BTC, BinanceSubscriptionType.TRADE));
        Thread.sleep(60000);
        for(Disposable d : m.listOfDisposables){
            d.dispose();
            LOG.info("Disposed of "+d.toString());
        }
        System.exit(0);
    }

    public Disposable addToStream(CurrencyPair cp, BinanceSubscriptionType subscriptionType){
        Disposable trades = exchange.getStreamingMarketDataService()
                .getTrades(cp)
                .doOnDispose(
                        () -> exchange.getStreamingMarketDataService().unsubscribe(cp, subscriptionType))
                .subscribe(trade -> { processTradeData(trade);});
        return trades;
    }

    public void processTradeData(Trade trade) throws IOException {
        appender.startExcerpt();
        String id = trade.getId();
        appender.writeUTF(id);
        appender.writeUTF(trade.getInstrument().toString());
        appender.writeUTF(trade.getType().name());
        if(trade.getMakerOrderId() != null){
            appender.writeUTF(trade.getMakerOrderId());
        }
        if(trade.getTakerOrderId() != null){
            appender.writeUTF(trade.getTakerOrderId());
        }
        appender.writeUTF(trade.getTimestamp().toString());
        appender.writeDouble(trade.getPrice().doubleValue());
        appender.writeDouble(trade.getOriginalAmount().doubleValue());
        appender.finish();
        LOG.info(String.format("Logged %s Trade: %s at %s", trade.getInstrument().toString(), trade.getPrice().doubleValue(), trade.getTimestamp().toString()));
    }


    public Disposable  createBinanceConnectionAndSubscribe(){
        ExchangeSpecification spec = StreamingExchangeFactory.INSTANCE.createExchange(BinanceStreamingExchange.class)
                .getDefaultExchangeSpecification();
        exchange = (BinanceStreamingExchange) StreamingExchangeFactory.INSTANCE.createExchange(spec);

        // First, we need to subscribe to at least one currency pair at connection time
        // Note: at connection time, the live subscription is disabled
        ProductSubscription subscription =
                ProductSubscription.create().addTrades(CurrencyPair.BTC_USDT).addOrderbook(CurrencyPair.BTC_USDT).build();
        exchange.connect(subscription).blockingAwait();

        // We subscribe to trades update for the currency pair subscribed at connection time (BTC)
        // For live unsubscription, you need to add a doOnDispose that will call the method unsubscribe in BinanceStreamingMarketDataService
        Disposable tradesBtc = exchange.getStreamingMarketDataService()
                .getTrades(CurrencyPair.BTC_USDT)
                .doOnDispose(
                        () -> exchange.getStreamingMarketDataService().unsubscribe(CurrencyPair.BTC_USDT, BinanceSubscriptionType.TRADE))
                .subscribe(trade -> { processTradeData(trade); });
        // Now we enable the live subscription/unsubscription to add new currencies to the streams
        exchange.enableLiveSubscription();
        return  tradesBtc;
    }

}