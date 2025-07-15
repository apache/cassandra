package org.apache.cassandra.profiler;
import org.apache.cassandra.tools.profiler.AsyncProfilerService;

public class AsyncProfiler implements AsyncProfilerMBean {
    public static final String MBEAN_NAME = "org.apache.cassandra.profiler:type=AsyncProfiler";
    private final AsyncProfilerService service = new AsyncProfilerService();

    public void start(String event) {
        service.start(event);
    }

    public void stop(String outputFile) {
        service.stop(outputFile);
    }

    public boolean isAvailable() {
        return service.isAvailable();
    }
}