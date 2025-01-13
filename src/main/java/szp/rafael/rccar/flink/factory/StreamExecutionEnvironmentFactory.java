package szp.rafael.rccar.flink.factory;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamExecutionEnvironmentFactory {
    public static StreamExecutionEnvironment createLocalEnvironment() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if(false) {
            env.enableCheckpointing(1000 * 5); // Checkpoint a cada 5 segundos
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // Modo de checkpoint

            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(50 * 100); // Pausa mínima entre checkpoints
            env.getCheckpointConfig().setCheckpointTimeout(1000 * 5 * 3); // Timeout de checkpoint
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
            // sets the checkpoint storage where checkpoint snapshots will be written
            Configuration config = new Configuration();
            config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///" + System.getProperty("java.io.tmpdir") + "/checkpoints");
            config.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
            env.configure(config);
        }
        return env;
    }
}
