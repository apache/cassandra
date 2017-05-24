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
import java.net.Socket;
import java.nio.channels.SocketChannel;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.streaming.StreamConnectionFactory;
import org.apache.cassandra.utils.FBUtilities;

public class BulkLoadConnectionFactory implements StreamConnectionFactory
{
    private final boolean outboundBindAny;
    private final int secureStoragePort;
    private final EncryptionOptions.ServerEncryptionOptions encryptionOptions;

    public BulkLoadConnectionFactory(int secureStoragePort, EncryptionOptions.ServerEncryptionOptions encryptionOptions, boolean outboundBindAny)
    {
        this.secureStoragePort = secureStoragePort;
        this.encryptionOptions = encryptionOptions;
        this.outboundBindAny = outboundBindAny;
    }

    public Socket createConnection(InetAddressAndPort peer) throws IOException
    {
        // Connect to secure port for all peers if ServerEncryptionOptions is configured other than 'none'
        // When 'all', 'dc' and 'rack', server nodes always have SSL port open, and since thin client like sstableloader
        // does not know which node is in which dc/rack, connecting to SSL port is always the option.
        if (encryptionOptions != null && encryptionOptions.internode_encryption != EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none)
        {
            //Old configurations that don't use StartTLS still need to get the port from the YAML or external config
            //Newer configurations are required to have StartTLS in order to use a different port for each instance.
            boolean useConfigPort = encryptionOptions.outgoing_encrypted_port_source == EncryptionOptions.ServerEncryptionOptions.OutgoingEncryptedPortSource.yaml;
            if (outboundBindAny)
                return SSLFactory.getSocket(encryptionOptions, peer.address, useConfigPort ? secureStoragePort : peer.port);
            else
                return SSLFactory.getSocket(encryptionOptions, peer.address, peer.port, FBUtilities.getJustLocalAddress(), 0);
        }
        else
        {
            Socket socket = SocketChannel.open(new InetSocketAddress(peer.address, peer.port)).socket();
            if (outboundBindAny && !socket.isBound())
                socket.bind(new InetSocketAddress(FBUtilities.getJustLocalAddress(), 0));
            return socket;
        }
    }
}
