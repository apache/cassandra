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

package org.apache.cassandra.net.async;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;

public class OutboundConnectionParamsTest
{
    static int version;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        version = MessagingService.current_version;
    }

    @Test (expected = IllegalArgumentException.class)
    public void build_SendSizeLessThanZero()
    {
        OutboundConnectionParams.builder().protocolVersion(version).sendBufferSize(-1).build();
    }

    @Test (expected = IllegalArgumentException.class)
    public void build_SendSizeHuge()
    {
        OutboundConnectionParams.builder().protocolVersion(version).sendBufferSize(1 << 30).build();
    }

    @Test (expected = IllegalArgumentException.class)
    public void build_TcpConnectTimeoutLessThanZero()
    {
        OutboundConnectionParams.builder().protocolVersion(version).tcpConnectTimeoutInMS(-1).build();
    }

    @Test (expected = IllegalArgumentException.class)
    public void build_TcpUserTimeoutLessThanZero()
    {
        OutboundConnectionParams.builder().protocolVersion(version).tcpUserTimeoutInMS(-1).build();
    }

    @Test
    public void build_TcpUserTimeoutEqualsZero()
    {
        OutboundConnectionParams.builder().protocolVersion(version).tcpUserTimeoutInMS(0).build();
    }
}
