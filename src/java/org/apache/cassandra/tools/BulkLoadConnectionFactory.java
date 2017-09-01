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

import io.netty.channel.Channel;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.streaming.DefaultConnectionFactory;
import org.apache.cassandra.streaming.StreamConnectionFactory;

public class BulkLoadConnectionFactory extends DefaultConnectionFactory implements StreamConnectionFactory
{
    private final boolean outboundBindAny;
    private final int storagePort;
    private final int secureStoragePort;
    private final EncryptionOptions.ServerEncryptionOptions encryptionOptions;

    public BulkLoadConnectionFactory(int storagePort, int secureStoragePort, EncryptionOptions.ServerEncryptionOptions encryptionOptions, boolean outboundBindAny)
    {
        this.storagePort = storagePort;
        this.secureStoragePort = secureStoragePort;
        this.encryptionOptions = encryptionOptions != null && encryptionOptions.internode_encryption == EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none
                                 ? null
                                 : encryptionOptions;
        this.outboundBindAny = outboundBindAny;
    }

    public Channel createConnection(OutboundConnectionIdentifier connectionId, int protocolVersion) throws IOException
    {
        // Connect to secure port for all peers if ServerEncryptionOptions is configured other than 'none'
        // When 'all', 'dc' and 'rack', server nodes always have SSL port open, and since thin client like sstableloader
        // does not know which node is in which dc/rack, connecting to SSL port is always the option.
        int port = encryptionOptions != null && encryptionOptions.internode_encryption != EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none ?
                   secureStoragePort : storagePort;

        connectionId = connectionId.withNewConnectionAddress(new InetSocketAddress(connectionId.remote(), port));
        return createConnection(connectionId, protocolVersion, encryptionOptions);
    }
}
