package org.apache.cassandra.tools.profiler;

import one.profiler.AsyncProfiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_LIB_PATH;
import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_ADVANCED_MODE;

public class AsyncProfilerService {
    private static final Logger logger = LoggerFactory.getLogger(AsyncProfilerService.class);
    private static final Set<String> ALLOWED_EVENTS = Set.of("cpu", "alloc", "lock", "wall", "nativemem", "cache-misses");

    private static AsyncProfiler profilerInstance;

    static {
        try {
            String asyncProfilerLibPath = new File(ASYNC_PROFILER_LIB_PATH.getString()).getAbsolutePath();
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

        try {
            String cmd = String.format("start,event=%s,fmt=%s", event, outputFormat);
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
        if (!ASYNC_PROFILER_ENABLED.getBoolean()){
            throw new IllegalStateException("async-profiler is not enabled.");
        } else if (profilerInstance == null) {
            throw new IllegalStateException("async-profiler is not initialized.");
        }
    }

    private void validateEvent(String event){
        if (!Arrays.stream(event.split(",")).filter(s -> !s.isEmpty()).allMatch(ALLOWED_EVENTS::contains)){
            throw new IllegalArgumentException(String.format("Event must be one or a combination of %s", ALLOWED_EVENTS.toString()));
        }
    }

    private void validateOutputFileName(String outputFile){
        if (outputFile.matches(".*\\s.*")){
            throw new IllegalArgumentException("Output file name must be a non-space-delimited string.");
        }
    }
}