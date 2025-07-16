package org.apache.cassandra.profiler;

public interface AsyncProfilerMBean {
    void start(String event);
    void stop(String outputFile);
    void execute(String command);
    boolean isAvailable();
}