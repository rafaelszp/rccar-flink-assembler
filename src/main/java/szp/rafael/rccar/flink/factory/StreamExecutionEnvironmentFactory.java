package szp.rafael.rccar.flink.factory;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

public class StreamExecutionEnvironmentFactory {

    private static final boolean ENABLE_CHECKPOINTS = true;
    private static final String ENABLE_CHECKPOINTS_PARAM = "--enable-checkpoints";
    public static final String CHECKPOINTS_FOLDER = "file:///" + System.getProperty("java.io.tmpdir") + "/checkpoints";


    public static StreamExecutionEnvironment createLocalEnvironment(ParameterTool params) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 30); // number of restart attempts
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(30)); // delay

        if(params.has(ENABLE_CHECKPOINTS_PARAM)) {
            env.enableCheckpointing(100 * 5); // Checkpoint em ms
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // Modo de checkpoint

            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(50 * 100); // Pausa mínima entre checkpoints
            env.getCheckpointConfig().setCheckpointTimeout(1000 * 5 * 3); // Timeout de checkpoint
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

        }
        env.configure(config);


        return env;
    }

    public static StreamExecutionEnvironment createLocalEnvironment() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 100); // number of restart attempts
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(3)); // delay

        if(ENABLE_CHECKPOINTS) {
            env.enableCheckpointing(100 * 10); // Checkpoint em ms
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // Modo de checkpoint

            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5 * 100); // Pausa mínima entre checkpoints
            env.getCheckpointConfig().setCheckpointTimeout(1000 * 60); // Timeout de checkpoint
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
            env.getCheckpointConfig().setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

            // sets the checkpoint storage where checkpoint snapshots will be written
            config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");

            config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, CHECKPOINTS_FOLDER);
            config.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
            config.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
            config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");

        }
        int defaultLocalParallelism = Runtime.getRuntime().availableProcessors();
        env.setParallelism(defaultLocalParallelism);
        config.setString("taskmanager.memory.network.max", "1gb");
        config.setString("taskmanager.memory.size", "1gb");
        env.configure(config);


        return env;
    }

    public static void main(String[] args) throws IOException {
        deleteDirectory(Paths.get(CHECKPOINTS_FOLDER));
    }

    public static void deleteDirectory(Path path) throws IOException {
        // Verifica se o diretório existe
        if (Files.exists(path)) {
            // Caminha pela árvore de diretórios e apaga os arquivos e subdiretórios
            Files.walk(path)
                    .sorted((a, b) -> b.compareTo(a)) // Ordena em ordem decrescente para apagar subdiretórios antes dos diretórios pai
                    .forEach(StreamExecutionEnvironmentFactory::deletePath);
        }
    }

    private static void deletePath(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            System.err.println("Erro ao deletar " + path + ": " + e.getMessage());
        }
    }

}
