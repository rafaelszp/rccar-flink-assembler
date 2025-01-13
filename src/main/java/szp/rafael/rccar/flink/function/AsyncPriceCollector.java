package szp.rafael.rccar.flink.function;

import com.eclipsesource.json.JsonObject;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.ResponseBody;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.CarSituation;
import szp.rafael.rccar.dto.RCCar;
import szp.rafael.rccar.flink.enums.PartType;
import szp.rafael.rccar.flink.util.RCCarConfig;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;


public class AsyncPriceCollector extends RichAsyncFunction<RCCar, RCCar> {

    private static final Logger logger = LoggerFactory.getLogger(AsyncPriceCollector.class);
    private transient OkHttpClient client;
    AtomicReference<RCCar> carState;


    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        client = new OkHttpClient();
        carState = new AtomicReference<>();
    }

    @Override
    public void asyncInvoke(RCCar rcCar, ResultFuture<RCCar> resultFuture) throws Exception {

        carState.set(RCCar.newBuilder(rcCar).build());

        getPrice(PartType.BODY, rcCar, resultFuture);
        getPrice(PartType.ENGINE, rcCar, resultFuture);
        getPrice(PartType.REMOTE_CONTROL, rcCar, resultFuture);
        getPrice(PartType.WHEEL, rcCar, resultFuture);
    }

    private void getPrice(PartType partType, RCCar rcCar, ResultFuture<RCCar> resultFuture) {
        String id = "";

        switch (partType) {
            case BODY:
                id = rcCar.getBody().getId().toString();
                break;
            case WHEEL:
                id = rcCar.getWheels().stream().findFirst().get().getId().toString();
                break;
            case REMOTE_CONTROL:
                id = rcCar.getRemoteControl().getId().toString();
                break;
            case ENGINE:
                id = rcCar.getEngine().getId().toString();
                break;
        }
        synchronized (partType) {
            client.newCall(RCCarConfig.createPriceRequest(id)).enqueue(new Callback() {

                @Override
                public void onFailure(okhttp3.Call call, IOException e) {
                    logger.warn("Error getting price for part: {} - {}", partType, e.getMessage());
                    resultFuture.completeExceptionally(e);
                }

                @Override
                public void onResponse(okhttp3.Call call, okhttp3.Response response) throws IOException {
                    var updatedCar = RCCar.newBuilder(carState.get());
                    try (ResponseBody body = response.body()) {
                        if (!response.isSuccessful()) {
                            throw new IOException("Unexpected code " + response);
                        }
                        JsonObject json = JsonObject.readFrom(body.string());
                        var price = BigDecimal.valueOf(json.get("price").asDouble()).setScale(2, RoundingMode.HALF_EVEN);

                        switch (partType) {
                            case BODY:
                                updatedCar.getBody().getPart().setPrice(price.doubleValue());
//                                logger.info("Body price: {} - sku: {}", price,updatedCar.getSku());
                                break;
                            case WHEEL:
                                for (int i = 0; i < updatedCar.getWheels().size(); i++) {
                                    double wheelsPrice = price.doubleValue();
                                    updatedCar.getWheels().get(i).getPart().setPrice(wheelsPrice);
//                                    logger.info("Wheel price: {} - sku: {}", wheelsPrice,updatedCar.getSku());
                                }
                                price = price.multiply(BigDecimal.valueOf(4));
                                break;
                            case REMOTE_CONTROL:
                                updatedCar.getRemoteControl().getPart().setPrice(price.doubleValue());
//                                logger.info("Remote control price: {} - sku: {}", price,updatedCar.getSku());
                                break;
                            case ENGINE:
                                updatedCar.getEngine().getPart().setPrice(price.doubleValue());
//                                logger.info("Engine price: {} - sku: {}", price,updatedCar.getSku());
                                break;
                        }
                        Double totalPrice = carState.get().getTotalPrice() + price.doubleValue();
                        updatedCar.setTotalPrice(BigDecimal.valueOf(totalPrice).setScale(2, RoundingMode.HALF_EVEN).doubleValue());
                        carState.set(updatedCar.build());

                        if (isComplete()) {
                            carState.set(updatedCar.setSituation(CarSituation.COMPLETE).build());
//                            logger.info("Completing transaction. Total price: {}. Car: {}.", totalPrice, carState.get());
                            resultFuture.complete(Collections.singleton(carState.get()));
                        }
                    }
                }
            });
        }
    }

    private boolean isComplete() {
        RCCar rcCar = carState.get();
        return rcCar.getBody().getPart().getPrice() > 0.0
                && rcCar.getEngine().getPart().getPrice() > 0.0
                && rcCar.getRemoteControl().getPart().getPrice() > 0.0
                && rcCar.getWheels().stream().reduce(0.0, (acc, w) -> acc + w.getPart().getPrice(), Double::sum) > 0.0;
    }


    @Override
    public void timeout(RCCar input, ResultFuture<RCCar> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
        logger.warn("TIMEOUT: {}", input.getSku());
        var fallback = RCCar.newBuilder(input);
        fallback.setTotalPrice(-1.0);
        resultFuture.complete(Collections.singleton(fallback.build()));
    }


}
