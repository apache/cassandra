package org.apache.cassandra.profiler;

public interface AsyncProfilerMBean {
    void start(String event, String outputFormat);
    void stop(String outputFile);
    void execute(String command);
    boolean isAvailable();
}
