package org.apache.cassandra.tools.profiler;

import one.profiler.AsyncProfiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class AsyncProfilerService {
    private static final Logger logger = LoggerFactory.getLogger(AsyncProfilerService.class);
    private static AsyncProfiler profilerInstance;

    static {
        try {
            String asyncProfilerLibPath = new File(System.getProperty("cassandra.async_profiler.library_path")).getAbsolutePath();
            profilerInstance = AsyncProfiler.getInstance(asyncProfilerLibPath);
        } catch (Throwable t) {
            System.out.println("async-profiler ERROR");
            t.printStackTrace();
            profilerInstance = null;
        }
    }

    public void start(String event) {
        if (profilerInstance == null) {
            throw new IllegalStateException("async-profiler is not initialized.");
        }
        try {
            profilerInstance.execute("start,event=" + event);
            logger.info("Started async-profiler: event={}", event);
        } catch (IOException e) {
            logger.error("Failed to start async-profiler", e);
            throw new RuntimeException(e);
        }
    }

    public void stop(String outputFile) {
        if (profilerInstance == null) {
            throw new IllegalStateException("async-profiler is not initialized.");
        }
        try {
            profilerInstance.execute("stop,file=" + outputFile);
            logger.info("Stopped async-profiler and wrote output to {}", outputFile);
        } catch (IOException e) {
            logger.error("Failed to stop async-profiler", e);
            throw new RuntimeException(e);
        }
    }

    public boolean isAvailable() {
        return profilerInstance != null;
    }
}