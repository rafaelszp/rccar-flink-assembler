package szp.rafael.rccar.flink.processor;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.CarSituation;
import szp.rafael.rccar.dto.RCCar;
import szp.rafael.rccar.flink.factory.KafkaSinkFactory;
import szp.rafael.rccar.flink.stream.routing.CarSituationRoute;
import szp.rafael.rccar.flink.util.RCCarConfig;

//public class CarSituationProcessor extends KeyedBroadcastProcessFunction<String, RCCar, CarSituationRoute,Void> {
public class CarSituationProcessor extends BroadcastProcessFunction<RCCar, CarSituationRoute, Void> {

    private transient MapStateDescriptor<CarSituation,CarSituationRoute> routingDescriptor;
    private static final Logger logger = LoggerFactory.getLogger(CarSituationProcessor.class);
    private transient KafkaSink<RCCar> kafkaIncompleteSink;
    private transient KafkaSink<RCCar> kafkaCompleteSink;

    public CarSituationProcessor() {
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        kafkaIncompleteSink = KafkaSinkFactory.createKafkaSink(RCCarConfig.RCCAR_INCOMPLETE);
        kafkaCompleteSink = KafkaSinkFactory.createKafkaSink(RCCarConfig.RCCAR_COMPLETE);
        this.routingDescriptor = createRoutingDescriptor();
    }

    /*
     * Processing broadcasted rules and adding to the state*/
    @Override
    public void processBroadcastElement(CarSituationRoute carSituationRoute, BroadcastProcessFunction<RCCar, CarSituationRoute, Void>.Context context, Collector<Void> collector) throws Exception {
        var rulesState = getBroadcastState(context);
        logger.info("Processing situation {} for car", carSituationRoute);
        rulesState.put(carSituationRoute.getCarSituation(), carSituationRoute);
    }

    @Override
    public void processElement(RCCar rcCar, BroadcastProcessFunction<RCCar, CarSituationRoute, Void>.ReadOnlyContext readOnlyContext, Collector<Void> collector) throws Exception {
        var carSituationRoute = readOnlyContext.getBroadcastState(routingDescriptor).get(rcCar.getSituation());
        logger.info("Processing RCcar situation {} for car {}", carSituationRoute, rcCar);
        if(carSituationRoute.getCarSituation().equals(CarSituation.COMPLETE)){

        }else{

        }
    }

    private BroadcastState<CarSituation,CarSituationRoute> getBroadcastState(Context ctx) {
        return ctx.getBroadcastState(this.routingDescriptor);
    }

    public static MapStateDescriptor<CarSituation,CarSituationRoute> createRoutingDescriptor() {
        return new MapStateDescriptor<>("car-situations", Types.ENUM(CarSituation.class), Types.POJO(CarSituationRoute.class));
    }
}

