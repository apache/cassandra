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

package org.apache.cassandra.net;

import java.util.function.Function;

import com.google.common.base.Preconditions;

import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static org.apache.cassandra.net.MessagingService.*;

public class InboundConnectionSettings
{
    public final IInternodeAuthenticator authenticator;
    public final InetAddressAndPort bindAddress;
    public final ServerEncryptionOptions encryption;
    public final Integer socketReceiveBufferSizeInBytes;
    public final Integer applicationReceiveQueueCapacityInBytes;
    public final AcceptVersions acceptMessaging;
    public final AcceptVersions acceptStreaming;
    public final SocketFactory socketFactory;
    public final Function<InetAddressAndPort, InboundMessageHandlers> handlers;

    private InboundConnectionSettings(IInternodeAuthenticator authenticator,
                                      InetAddressAndPort bindAddress,
                                      ServerEncryptionOptions encryption,
                                      Integer socketReceiveBufferSizeInBytes,
                                      Integer applicationReceiveQueueCapacityInBytes,
                                      AcceptVersions acceptMessaging,
                                      AcceptVersions acceptStreaming,
                                      SocketFactory socketFactory,
                                      Function<InetAddressAndPort, InboundMessageHandlers> handlers)
    {
        this.authenticator = authenticator;
        this.bindAddress = bindAddress;
        this.encryption = encryption;
        this.socketReceiveBufferSizeInBytes = socketReceiveBufferSizeInBytes;
        this.applicationReceiveQueueCapacityInBytes = applicationReceiveQueueCapacityInBytes;
        this.acceptMessaging = acceptMessaging;
        this.acceptStreaming = acceptStreaming;
        this.socketFactory = socketFactory;
        this.handlers = handlers;
    }

    public InboundConnectionSettings()
    {
        this(null, null, null, null, null, null, null, null, null);
    }

    public String toString()
    {
        return format("address: (%s), nic: %s, encryption: %s",
                      bindAddress, FBUtilities.getNetworkInterface(bindAddress.getAddress()), SocketFactory.encryptionOptionsSummary(encryption));
    }

    public InboundConnectionSettings withAuthenticator(IInternodeAuthenticator authenticator)
    {
        return new InboundConnectionSettings(authenticator, bindAddress, encryption,
                                             socketReceiveBufferSizeInBytes, applicationReceiveQueueCapacityInBytes,
                                             acceptMessaging, acceptStreaming, socketFactory, handlers);
    }

    @SuppressWarnings("unused")
    public InboundConnectionSettings withBindAddress(InetAddressAndPort bindAddress)
    {
        return new InboundConnectionSettings(authenticator, bindAddress, encryption,
                                             socketReceiveBufferSizeInBytes, applicationReceiveQueueCapacityInBytes,
                                             acceptMessaging, acceptStreaming, socketFactory, handlers);
    }

    public InboundConnectionSettings withEncryption(ServerEncryptionOptions encryption)
    {
        return new InboundConnectionSettings(authenticator, bindAddress, encryption,
                                             socketReceiveBufferSizeInBytes, applicationReceiveQueueCapacityInBytes,
                                             acceptMessaging, acceptStreaming, socketFactory, handlers);
    }

    public InboundConnectionSettings withSocketReceiveBufferSizeInBytes(int socketReceiveBufferSizeInBytes)
    {
        return new InboundConnectionSettings(authenticator, bindAddress, encryption,
                                             socketReceiveBufferSizeInBytes, applicationReceiveQueueCapacityInBytes,
                                             acceptMessaging, acceptStreaming, socketFactory, handlers);
    }

    @SuppressWarnings("unused")
    public InboundConnectionSettings withApplicationReceiveQueueCapacityInBytes(int applicationReceiveQueueCapacityInBytes)
    {
        return new InboundConnectionSettings(authenticator, bindAddress, encryption,
                                             socketReceiveBufferSizeInBytes, applicationReceiveQueueCapacityInBytes,
                                             acceptMessaging, acceptStreaming, socketFactory, handlers);
    }

    public InboundConnectionSettings withAcceptMessaging(AcceptVersions acceptMessaging)
    {
        return new InboundConnectionSettings(authenticator, bindAddress, encryption,
                                             socketReceiveBufferSizeInBytes, applicationReceiveQueueCapacityInBytes,
                                             acceptMessaging, acceptStreaming, socketFactory, handlers);
    }

    public InboundConnectionSettings withAcceptStreaming(AcceptVersions acceptMessaging)
    {
        return new InboundConnectionSettings(authenticator, bindAddress, encryption,
                                             socketReceiveBufferSizeInBytes, applicationReceiveQueueCapacityInBytes,
                                             acceptMessaging, acceptStreaming, socketFactory, handlers);
    }

    public InboundConnectionSettings withSocketFactory(SocketFactory socketFactory)
    {
        return new InboundConnectionSettings(authenticator, bindAddress, encryption,
                                             socketReceiveBufferSizeInBytes, applicationReceiveQueueCapacityInBytes,
                                             acceptMessaging, acceptStreaming, socketFactory, handlers);
    }

    public InboundConnectionSettings withHandlers(Function<InetAddressAndPort, InboundMessageHandlers> handlers)
    {
        return new InboundConnectionSettings(authenticator, bindAddress, encryption,
                                             socketReceiveBufferSizeInBytes, applicationReceiveQueueCapacityInBytes,
                                             acceptMessaging, acceptStreaming, socketFactory, handlers);
    }

    public InboundConnectionSettings withLegacySslStoragePortDefaults()
    {
        ServerEncryptionOptions encryption = this.encryption;
        if (encryption == null)
            encryption = DatabaseDescriptor.getInternodeMessagingEncyptionOptions();
        encryption = encryption.withOptional(false).withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all);

        return this.withBindAddress(bindAddress.withPort(DatabaseDescriptor.getSSLStoragePort()))
                   .withEncryption(encryption)
                   .withDefaults();
    }

    // note that connectTo is updated even if specified, in the case of pre40 messaging and using encryption (to update port)
    public InboundConnectionSettings withDefaults()
    {
        // this is for the socket that can be plain, only ssl, or optional plain/ssl
        if (bindAddress.getPort() != DatabaseDescriptor.getStoragePort() && bindAddress.getPort() != DatabaseDescriptor.getSSLStoragePort())
            throw new ConfigurationException(format("Local endpoint port %d doesn't match YAML configured port %d or legacy SSL port %d",
                                                    bindAddress.getPort(), DatabaseDescriptor.getStoragePort(), DatabaseDescriptor.getSSLStoragePort()));

        IInternodeAuthenticator authenticator = this.authenticator;
        ServerEncryptionOptions encryption = this.encryption;
        Integer socketReceiveBufferSizeInBytes = this.socketReceiveBufferSizeInBytes;
        Integer applicationReceiveQueueCapacityInBytes = this.applicationReceiveQueueCapacityInBytes;
        AcceptVersions acceptMessaging = this.acceptMessaging;
        AcceptVersions acceptStreaming = this.acceptStreaming;
        SocketFactory socketFactory = this.socketFactory;
        Function<InetAddressAndPort, InboundMessageHandlers> handlersFactory = this.handlers;

        if (authenticator == null)
            authenticator = DatabaseDescriptor.getInternodeAuthenticator();

        if (encryption == null)
            encryption = DatabaseDescriptor.getInternodeMessagingEncyptionOptions();

        if (socketReceiveBufferSizeInBytes == null)
            socketReceiveBufferSizeInBytes = DatabaseDescriptor.getInternodeSocketReceiveBufferSizeInBytes();

        if (applicationReceiveQueueCapacityInBytes == null)
            applicationReceiveQueueCapacityInBytes = DatabaseDescriptor.getInternodeApplicationReceiveQueueCapacityInBytes();

        if (acceptMessaging == null)
            acceptMessaging = accept_messaging;

        if (acceptStreaming == null)
            acceptStreaming = accept_streaming;

        if (socketFactory == null)
            socketFactory = instance().socketFactory;

        if (handlersFactory == null)
            handlersFactory = instance()::getInbound;

        Preconditions.checkArgument(socketReceiveBufferSizeInBytes == 0 || socketReceiveBufferSizeInBytes >= 1 << 10, "illegal socket send buffer size: " + socketReceiveBufferSizeInBytes);
        Preconditions.checkArgument(applicationReceiveQueueCapacityInBytes >= 1 << 10, "illegal application receive queue capacity: " + applicationReceiveQueueCapacityInBytes);

        return new InboundConnectionSettings(authenticator, bindAddress, encryption, socketReceiveBufferSizeInBytes, applicationReceiveQueueCapacityInBytes, acceptMessaging, acceptStreaming, socketFactory, handlersFactory);
    }
}
