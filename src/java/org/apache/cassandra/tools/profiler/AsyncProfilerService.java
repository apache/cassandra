package org.apache.cassandra.tools.profiler;

import one.profiler.AsyncProfiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File; //checkstyle: permit this import
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_LIB_PATH;
import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_ADVANCED_MODE;

public class AsyncProfilerService {
    private static final Logger logger = LoggerFactory.getLogger(AsyncProfilerService.class);

    private static final Set<String> VALID_EVENTS = Set.of("cpu", "alloc", "lock", "wall", "nativemem", "cache-misses");
    private static final Set<String> VALID_FORMATS = Set.of("flat","traces","collapsed","flamegraph","tree","jfr","otlp");
    private static final Character[] INVALID_OUTPUT_FILENAME_CHARS = {'"', '*', '<', '>', '?', '|'};

    private static AsyncProfiler profilerInstance;

    static {
        try {
            String asyncProfilerLibPath = new File(ASYNC_PROFILER_LIB_PATH.getString()).getAbsolutePath(); //checkstyle: permit this instantiation
            profilerInstance = AsyncProfiler.getInstance(asyncProfilerLibPath);
        } catch (Throwable t) {
            System.out.println("async-profiler initialization ERROR");
            t.printStackTrace();
            profilerInstance = null;
        }
    }

    public void start(String event, String outputFormat) {
        checkProfilerInstance();
        validateEvent(event);
        validateFormat(outputFormat);

        try {
            String cmd = String.format("start,event=%s,output=%s", event, outputFormat);
            profilerInstance.execute(cmd);
            logger.info("Started async-profiler: cmd={}", cmd);
        } catch (IOException e) {
            logger.error("Failed to start async-profiler", e);
            throw new RuntimeException(e);
        }
    }

    public void stop(String outputFile) {
        checkProfilerInstance();
        validateOutputFileName(outputFile);

        try {
            profilerInstance.execute("stop,file=" + outputFile);
            logger.info("Stopped async-profiler and wrote output to {}", outputFile);
        } catch (IOException e) {
            logger.error("Failed to stop async-profiler", e);
            throw new RuntimeException(e);
        }
    }

    public void execute(String command) {
        checkProfilerInstance();

        try {
            if (!ASYNC_PROFILER_ADVANCED_MODE.getBoolean()){
                throw new IllegalStateException("ASYNC_PROFILER_ADVANCED_MODE must be set to true to execute raw commands.");
            }
            profilerInstance.execute(command);
            logger.info("Executed raw async-profiler command {}", command);
        } catch (IOException e) {
            logger.error("Failed to execute raw async-profiler command {}", command, e);
            throw new RuntimeException(e);
        }
    }

    public boolean isAvailable() {
        return profilerInstance != null;
    }

    private void checkProfilerInstance() {
        if (ASYNC_PROFILER_ENABLED.getBoolean() == false){
            throw new IllegalStateException("async-profiler is not enabled.");
        } else if (!isAvailable()) {
            throw new IllegalStateException("async-profiler is not initialized.");
        }
    }

    private void validateEvent(String event){
        if (!Arrays.stream(event.split(",")).filter(s -> !s.isEmpty()).allMatch(VALID_EVENTS::contains)){
            throw new IllegalArgumentException(String.format("Event must be one or a combination of %s", VALID_EVENTS.toString()));
        }
    }

    private void validateFormat(String format){
        if (!VALID_FORMATS.contains(format)){
            throw new IllegalArgumentException(String.format("Format must be one or a combination of %s", VALID_FORMATS.toString()));
        }
    }

    private void validateOutputFileName(String outputFile){
        if (outputFile == null || outputFile.trim().isEmpty()) {
            throw new IllegalArgumentException("Output file name must not be null or empty.");
        }
        if (Arrays.stream(INVALID_OUTPUT_FILENAME_CHARS).anyMatch(ch -> outputFile.contains(ch.toString()))){
            throw new IllegalArgumentException(String.format("Output file name must not contain any invalid characters %s", INVALID_OUTPUT_FILENAME_CHARS.toString()));
        }
    }
}
