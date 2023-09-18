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

import io.netty.channel.Channel;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.net.OutboundConnectionSettings;
import org.apache.cassandra.streaming.DefaultConnectionFactory;
import org.apache.cassandra.streaming.StreamConnectionFactory;

public class BulkLoadConnectionFactory extends DefaultConnectionFactory implements StreamConnectionFactory
{
    private final int storagePort;
    private final EncryptionOptions.ServerEncryptionOptions encryptionOptions;

    public BulkLoadConnectionFactory(EncryptionOptions.ServerEncryptionOptions encryptionOptions, int storagePort)
    {
        this.storagePort = storagePort;
        this.encryptionOptions = encryptionOptions;
    }

    public Channel createConnection(OutboundConnectionSettings template, int messagingVersion) throws IOException
    {
        // storage port can handle both encrypted and unencrypted traffic from 4.0
        // so from sstableloader's point of view we can use just storage port for both cases

        template = template.withConnectTo(template.to.withPort(storagePort));

        if (encryptionOptions != null && encryptionOptions.internode_encryption != EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none)
            template = template.withEncryption(encryptionOptions);

        return super.createConnection(template, messagingVersion);
    }

    @Override
    public boolean supportsPreferredIp()
    {
        return false; // called in a tool context, do not use getPreferredIP
    }
}
