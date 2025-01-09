package szp.rafael.rccar.flink.function;

import com.eclipsesource.json.JsonObject;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.ResponseBody;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.shaded.zookeeper3.org.apache.jute.compiler.generated.Rcc;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.RCCar;
import szp.rafael.rccar.flink.util.RCCarConfig;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;

public class AsyncPriceCollector extends  RichAsyncFunction<RCCar, RCCar> {

    private static final Logger logger = LoggerFactory.getLogger(AsyncPriceCollector.class);
    private transient OkHttpClient client;



    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        client = new OkHttpClient();
    }

    @Override
    public void asyncInvoke(RCCar rcCar, ResultFuture<RCCar> resultFuture) throws Exception {
        getPrice(rcCar.getBody().getId().toString(), rcCar, resultFuture);
        getPrice(rcCar.getEngine().getId().toString(), rcCar, resultFuture);
        getPrice(rcCar.getRemoteControl().getId().toString(), rcCar, resultFuture);
        rcCar.getWheels().forEach(wheel -> getPrice(wheel.getId().toString(), rcCar, resultFuture));
    }

    private void getPrice(String id, RCCar rcCar, ResultFuture<RCCar> resultFuture) {
        client.newCall(RCCarConfig.createPriceRequest(id)).enqueue(new Callback(){
            @Override
            public void onFailure(okhttp3.Call call, IOException e) {
                resultFuture.completeExceptionally(e);
            }
            @Override
            public void onResponse(okhttp3.Call call, okhttp3.Response response) throws IOException {
                try(ResponseBody body = response.body()) {
                    if(!response.isSuccessful()){
                        throw new IOException("Unexpected code " + response);
                    }
                    JsonObject json = JsonObject.readFrom(body.string());
                    var price = BigDecimal.valueOf(json.get("price").asDouble()).setScale(2, RoundingMode.HALF_EVEN);
                    Double totalPrice = rcCar.getTotalPrice();
                    var updatedCar = RCCar.newBuilder(rcCar)
                            .setTotalPrice(totalPrice + price.doubleValue())
                            .build();
                    resultFuture.complete(Collections.singleton(updatedCar));
                }
            }
        });
    }
}
