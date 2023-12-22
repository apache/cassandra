/*
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

package org.apache.cassandra.distributed.impl;

import java.io.IOException;

import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeProbe;

import static org.apache.cassandra.distributed.api.Feature.JMX;

public class InternalNodeProbeFactory implements INodeProbeFactory
{
    private final boolean withNotifications;
    private final IInstanceConfig config;

    public InternalNodeProbeFactory(IInstanceConfig config, boolean withNotifications)
    {
        if (!config.has(JMX))
            throw new IllegalStateException(String.format("Please enable %s feature.", Feature.JMX));

        this.config = config;
        this.withNotifications = withNotifications;
    }

    @Override
    public NodeProbe create(String host, int port) throws IOException
    {
        return new InternalNodeProbe(host, port);
    }

    @Override
    public NodeProbe create(String host, int port, String username, String password) throws IOException
    {
        return new InternalNodeProbe(host, port, username, password);
    }

    private class InternalNodeProbe extends NodeProbe
    {
        public InternalNodeProbe(String host, int port) throws IOException
        {
            super(host, port);
        }

        public InternalNodeProbe(String host, int port, String username, String password) throws IOException
        {
            super(host, port, username, password);
        }

        @Override
        protected void connect() throws IOException
        {
            super.connect();

            if (!withNotifications)
                ssProxy.skipNotificationListeners(true);
        }

        @Override
        protected void initializeJMXConnector()
        {
            jmxc = JMXUtil.getJmxConnector(InternalNodeProbeFactory.this.config);
        }
    }
}
