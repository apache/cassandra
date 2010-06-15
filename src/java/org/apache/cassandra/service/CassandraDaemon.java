package org.apache.cassandra.service;

import java.io.IOException;

/**
 * The <code>CassandraDaemon</code> interface captures the lifecycle of a
 * Cassandra daemon that runs on a single node.
 * 
 */
public interface CassandraDaemon
{
    /**
     * Initialize the Cassandra Daemon based on the given <a
     * href="http://commons.apache.org/daemon/jsvc.html">Commons
     * Daemon</a>-specific arguments. To clarify, this is a hook for JSVC.
     * 
     * @param arguments
     *            the arguments passed in from JSVC
     * @throws IOException
     */
    public void init(String[] arguments) throws IOException;
    
    /**
     * Start the Cassandra Daemon, assuming that it has already been
     * initialized, via either {@link #init(String[])} or
     * {@link #load(String[])}.
     * 
     * @throws IOException
     */
    public void start() throws IOException;
    
    /**
     * Stop the daemon, ideally in an idempotent manner.
     */
    public void stop();
    
    /**
     * Clean up all resources obtained during the lifetime of the daemon. Just
     * to clarify, this is a hook for JSVC.
     */
    public void destroy();
    
    /**
     * A convenience method to initialize and start the daemon in one shot.
     */
    public void activate();
    
    /**
     * A convenience method to stop and destroy the daemon in one shot.
     */
    public void deactivate();
    
}