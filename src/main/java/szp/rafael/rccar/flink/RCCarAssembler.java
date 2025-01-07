package szp.rafael.rccar.flink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import szp.rafael.rccar.dto.Body;
import szp.rafael.rccar.dto.Engine;
import szp.rafael.rccar.dto.RemoteControl;
import szp.rafael.rccar.flink.stream.BodyEngineJoin;
import szp.rafael.rccar.flink.util.RCCarStreamFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class RCCarAssembler {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
                .map(tuple -> {
                    var body = tuple.f0;
                    var engine = tuple.f1 ;
                    var remoteControl = tuple.f2 ;
                    var wheels = tuple.f3 ;
                    System.out.println("body.toString() = " + body.toString());
                    return body.toString();
                })
                .sinkTo(sink);

//        bodyStream.print();
//        engineStream.print();
//        remoteControlStream.print();
//        wheelStream.print();

        env.execute("Flink avro rc assembler");

    }

}
