package org.apache.cassandra.tools.profiler;

import one.profiler.AsyncProfiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_LIB_PATH;
import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_ADVANCED_MODE;

public class AsyncProfilerService {
    private static final Logger logger = LoggerFactory.getLogger(AsyncProfilerService.class);
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

    public void start(String event) {
        checkProfilerInstance();

        try {
            profilerInstance.execute("start,event=" + event);
            logger.info("Started async-profiler: event={}", event);
        } catch (IOException e) {
            logger.error("Failed to start async-profiler", e);
            throw new RuntimeException(e);
        }
    }

    public void stop(String outputFile) {
        checkProfilerInstance();

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
}