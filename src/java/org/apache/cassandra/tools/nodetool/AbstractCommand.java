package org.apache.cassandra.tools.nodetool;

import javax.inject.Inject;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;
import picocli.CommandLine.ExecutionException;

/**
 * Abstract class for all nodetool commands, which provides common methods and fields.
 * <p>
 * The command is executed by calling {@link #execute(NodeProbe)}, in all other cases
 * it should not contain any fields or methods that are specific to a particular API
 * being executed, or common methods that are shared across multiple commands.
 * <p>
 * Commands must be API-agnostic and work only with the {@link NodeProbe} API, or a
 * wrapper around MBean classes (as a primary entry point), which do not need to be
 * initialized or used with JMX.
 */
public abstract class AbstractCommand implements Runnable
{
    @Inject
    protected Output output;

    private NodeProbe probe;

    public void probe(NodeProbe probe)
    {
        this.probe = probe;
    }

    public NodeProbe probe()
    {
        return probe;
    }

    public void logger(Output output)
    {
        this.output = output;
    }

    @Override
    public void run()
    {
        execute(probe());
    }

    /**
     * Prepare a command for execution. This method is called before the command is executed and
     * can be used to perform any necessary setup or validation. If this method returns {@code false},
     * the command will not initiate connection and will be executed locally. The default implementation
     * returns {@code true} so that the command initiates connection to the node before execution.
     *
     * @return {@code true} if the command is required to connect to the node, {@code false} otherwise.
     * @throws ExecutionException if an error occurs during preparation and execution must be aborted.
     */
    protected boolean prepareAndConnect() throws ExecutionException
    {
        return true;
    }

    /**
     * Execute the command using the supplied {@link NodeProbe} instance, which is already connected
     * to the node and ready to use. This method is called after the connection.
     *
     * @param probe The {@link NodeProbe} instance to use, or {@code null} if no connection is required.
     */
    protected abstract void execute(NodeProbe probe);
}