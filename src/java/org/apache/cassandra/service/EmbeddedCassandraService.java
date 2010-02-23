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


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An embedded, in-memory cassandra storage service that listens
 * on the thrift interface as configured in storage-conf.xml
 * This kind of service is useful when running unit tests of
 * services using cassandra for example.
 *
 * See {@link EmbeddedCassandraServiceTest} for usage.
 * <p>
 * This is the implementation of https://issues.apache.org/jira/browse/CASSANDRA-740
 * <p>
 * How to use:
 * In the client code create a new thread and spawn it with its {@link Thread#start()} method.
 * Example:
 * <pre>
 *      // Tell cassandra where the configuration files are.
        System.setProperty("storage-config", "conf");

        cassandra = new EmbeddedCassandraService();
        cassandra.init();

        // spawn cassandra in a new thread
        Thread t = new Thread(cassandra);
        t.setDaemon(true);
        t.start();

 * </pre>
 * @author Ran Tavory (rantav@gmail.com)
 *
 */
public class EmbeddedCassandraService implements Runnable
{

    CassandraDaemon cassandraDaemon;

    public void init() throws TTransportException, IOException
    {
        cassandraDaemon = new CassandraDaemon();
        cassandraDaemon.init(null);
    }

    public void run()
    {
        cassandraDaemon.start();
    }
}
