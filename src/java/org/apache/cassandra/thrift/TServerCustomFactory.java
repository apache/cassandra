/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.thrift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.server.TServer;

/**
 * Helper implementation to create a thrift TServer based on one of the common types we support (sync, hsha),
 * or a custom type by setting the fully qualified java class name in the rpc_server_type setting.
 */
public class TServerCustomFactory implements TServerFactory
{
    private static Logger logger = LoggerFactory.getLogger(TServerCustomFactory.class);
    private final String serverType;

    public TServerCustomFactory(String serverType)
    {
        assert serverType != null;
        this.serverType = serverType;
    }

    public TServer buildTServer(TServerFactory.Args args)
    {
        TServer server;
        if (ThriftServer.SYNC.equalsIgnoreCase(serverType))
        {
            server = new CustomTThreadPoolServer.Factory().buildTServer(args);
        }
        else if(ThriftServer.ASYNC.equalsIgnoreCase(serverType))
        {
            server = new CustomTNonBlockingServer.Factory().buildTServer(args);
            logger.info(String.format("Using non-blocking/asynchronous thrift server on %s : %s", args.addr.getHostName(), args.addr.getPort()));
        }
        else if(ThriftServer.HSHA.equalsIgnoreCase(serverType))
        {
            server = new THsHaDisruptorServer.Factory().buildTServer(args);
            logger.info(String.format("Using custom half-sync/half-async thrift server on %s : %s", args.addr.getHostName(), args.addr.getPort()));
        }
        else
        {
            TServerFactory serverFactory;
            try
            {
                serverFactory = (TServerFactory) Class.forName(serverType).newInstance();
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to instantiate server factory:" + serverType, e);
            }
            server = serverFactory.buildTServer(args);
            logger.info(String.format("Using custom thrift server %s on %s : %s", server.getClass().getName(), args.addr.getHostName(), args.addr.getPort()));
        }
        return server;
    }
}
