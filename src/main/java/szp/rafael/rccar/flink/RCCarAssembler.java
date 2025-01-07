package szp.rafael.rccar.flink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import szp.rafael.rccar.dto.RCCar;
import szp.rafael.rccar.flink.stream.BodyEngineJoin;
import szp.rafael.rccar.flink.stream.RCCarRemoteControlJoin;
import szp.rafael.rccar.flink.stream.RCCarWheelsJoin;
import szp.rafael.rccar.flink.util.RCCarStreamFactory;
import org.apache.flink.streaming.api.CheckpointingMode;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class RCCarAssembler {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000*600); // Checkpoint a cada 60 segundos
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // Modo de checkpoint

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500); // Pausa mínima entre checkpoints
        env.getCheckpointConfig().setCheckpointTimeout(1000*5*60); // Timeout de checkpoint
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        // sets the checkpoint storage where checkpoint snapshots will be written
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///"+System.getProperty("java.io.tmpdir")+"/checkpoints");
        config.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env.configure(config);


        var bodyStream = RCCarStreamFactory.createBodyStream(env);
        var engineStream = RCCarStreamFactory.createEngineStream(env);
        var remoteControlStream = RCCarStreamFactory.createRemoteControlStream(env);
        var wheelStream = RCCarStreamFactory.createWheelStream(env);

        String outputPath = System.getProperty("java.io.tmpdir")+ File.separator+"rccar-assembly";
        final FileSink<String> sink = FileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(1))
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(2))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        bodyStream.connect(engineStream)
                .keyBy(b->b.getPart().getSku(), e->e.getPart().getSku())
                .process(new BodyEngineJoin())
                .connect(remoteControlStream)
                .keyBy(RCCar::getSku, r->r.getPart().getSku())
                .process(new RCCarRemoteControlJoin())
                .connect(wheelStream)
                .keyBy(RCCar::getSku, w->w.getPart().getSku())
                .process(new RCCarWheelsJoin())
                .map(rccar -> {
                    var body = rccar.getBody();
                    var engine = rccar.getEngine() ;
                    var remoteControl = rccar.getRemoteControl() ;
                    var wheels = rccar.getWheels() ;
                    System.out.printf("rccar: %s:\n", rccar);
                    return rccar.toString();
                })

                .sinkTo(sink);

        env.execute("Flink avro rc assembler");

    }

}
