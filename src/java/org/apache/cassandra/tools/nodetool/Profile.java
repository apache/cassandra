package org.apache.cassandra.tools.nodetool;

import org.apache.cassandra.profiler.AsyncProfilerMBean;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.NodeProbe;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "profile", description = "Control async-profiler on this node")
public class Profile extends NodeTool.NodeToolCmd {

    @Option(name = {"-s", "--start"}, description = "Start profiling", arity = 0)
    public boolean start;

    @Option(name = {"-x", "--stop"}, description = "Stop profiling and dump output", arity = 0)
    public boolean stop;

    @Option(name = {"-e", "--event"}, description = "Event to profile (cpu, alloc, lock, wall, etc.)")
    public String event = "cpu";

    @Option(name = {"-f", "--file"}, description = "Output file for profile dump")
    public String outputFile = "/tmp/profile.html";

    @Override
    public void execute(NodeProbe probe) {
        AsyncProfilerMBean profiler = probe.getAsyncProfilerProxy();
        if (!profiler.isAvailable()) {
            System.err.println("Async-profiler native library is not loaded or unavailable.");
            return;
        }

        try {
            if (start) {
                System.out.printf("Starting async-profiler: event=%s\n", event);
                profiler.start(event);
            } else if (stop) {
                System.out.printf("Stopping profiler and writing output to: %s\n", outputFile);
                profiler.stop(outputFile);
            } else {
                System.out.println("Use --start or --stop to control profiling.");
            }
        } catch (Exception e) {
            System.err.println("Error while controlling profiler: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
