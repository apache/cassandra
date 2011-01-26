package org.apache.cassandra.service;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


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

    public void startRPCServer();
    public void stopRPCServer();
    public boolean isRPCServerRunning();
    
    /**
     * A convenience method to initialize and start the daemon in one shot.
     */
    public void activate();
    
    /**
     * A convenience method to stop and destroy the daemon in one shot.
     */
    public void deactivate();
    
}
