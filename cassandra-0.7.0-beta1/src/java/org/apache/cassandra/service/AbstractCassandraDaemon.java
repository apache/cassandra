/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>CassandraDaemon</code> is an abstraction for a Cassandra daemon
 * service, which defines not only a way to activate and deactivate it, but also
 * hooks into its lifecycle methods (see {@link #setup()}, {@link #start()},
 * {@link #stop()} and {@link #setup()}).
 * 
 */
public abstract class AbstractCassandraDaemon implements CassandraDaemon
{
    private static Logger logger = LoggerFactory
            .getLogger(AbstractCassandraDaemon.class);
    
    /**
     * This is a hook for concrete daemons to initialize themselves suitably.
     * 
     * @throws IOException
     */
    protected abstract void setup() throws IOException;
    
    /**
     * Initialize the Cassandra Daemon based on the given <a
     * href="http://commons.apache.org/daemon/jsvc.html">Commons
     * Daemon</a>-specific arguments. To clarify, this is a hook for JSVC.
     * 
     * @param arguments
     *            the arguments passed in from JSVC
     * @throws IOException
     */
    public void init(String[] arguments) throws IOException
    {
        setup();
    }
    
    /**
     * Start the Cassandra Daemon, assuming that it has already been
     * initialized, via either {@link #init(String[])} or
     * {@link #load(String[])}.
     * 
     * @throws IOException
     */
    public abstract void start() throws IOException;
    
    /**
     * Stop the daemon, ideally in an idempotent manner.
     */
    public abstract void stop();
    
    /**
     * Clean up all resources obtained during the lifetime of the daemon. This
     * is a hook for JSVC.
     */
    public void destroy()
    {}
    
    /**
     * A convenience method to initialize and start the daemon in one shot.
     */
    public void activate()
    {
        String pidFile = System.getProperty("cassandra-pidfile");
        
        try
        {
            setup();
            
            if (pidFile != null)
            {
                new File(pidFile).deleteOnExit();
            }
            
            if (System.getProperty("cassandra-foreground") == null)
            {
                System.out.close();
                System.err.close();
            }
            
            start();
        } catch (Throwable e)
        {
            String msg = "Exception encountered during startup.";
            logger.error(msg, e);
            
            // try to warn user on stdout too, if we haven't already detached
            System.out.println(msg);
            e.printStackTrace();
            
            System.exit(3);
        }
    }
    
    /**
     * A convenience method to stop and destroy the daemon in one shot.
     */
    public void deactivate()
    {
        stop();
        destroy();
    }
    
}
