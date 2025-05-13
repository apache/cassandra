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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.net.OutboundConnectionSettings;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.streaming.async.NettyStreamingChannel;

import static org.apache.cassandra.locator.InetAddressAndPort.getByAddress;

public class BulkLoadConnectionFactory extends NettyStreamingConnectionFactory
{
    private final int storagePort;
    private final EncryptionOptions.ServerEncryptionOptions encryptionOptions;

    public BulkLoadConnectionFactory(EncryptionOptions.ServerEncryptionOptions encryptionOptions, int storagePort)
    {
        this.storagePort = storagePort;
        this.encryptionOptions = encryptionOptions;
    }

    @Override
    public NettyStreamingChannel create(InetSocketAddress to, int messagingVersion, StreamingChannel.Kind kind) throws IOException
    {
        OutboundConnectionSettings template = new OutboundConnectionSettings(getByAddress(to));
        return create(template, messagingVersion, kind);
    }

    @Override
    public StreamingChannel create(InetSocketAddress to,
                                   InetSocketAddress preferred,
                                   int messagingVersion,
                                   StreamingChannel.Kind kind) throws IOException
    {
        // The preferred address is always overwritten in create(). This method override only exists so we can avoid
        // falling back to the NettyStreamingConnectionFactory implementation.
        OutboundConnectionSettings template = new OutboundConnectionSettings(getByAddress(to), getByAddress(preferred));
        return create(template, messagingVersion, kind);
    }

    private NettyStreamingChannel create(OutboundConnectionSettings template, int messagingVersion, StreamingChannel.Kind kind) throws IOException
    {
        // storage port can handle both encrypted and unencrypted traffic from 4.0
        // so from sstableloader's point of view we can use just storage port for both cases

        template = template.withConnectTo(template.to.withPort(storagePort));

        if (encryptionOptions != null && encryptionOptions.internode_encryption != EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none)
            template = template.withEncryption(encryptionOptions);

        return connect(template, messagingVersion, kind);
    }
    @Override
    public boolean supportsPreferredIp()
    {
        return false; // called in a tool context, do not use getPreferredIP
    }
}
