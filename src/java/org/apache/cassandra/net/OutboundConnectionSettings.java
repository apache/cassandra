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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.netty.channel.WriteBufferWaterMark;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.DatabaseDescriptor.getEndpointSnitch;
import static org.apache.cassandra.net.MessagingService.instance;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

/**
 * A collection of settings to be passed around for outbound connections.
 */
@SuppressWarnings({ "WeakerAccess", "unused" })
public class OutboundConnectionSettings
{
    /**
     * Enabled/disable TCP_NODELAY for intradc connections. Defaults to enabled.
     */
    private static final boolean INTRADC_TCP_NODELAY = CassandraRelevantProperties.OTC_INTRADC_TCP_NODELAY.getBoolean();

    public enum Framing
    {
        // uses simple unprotected frames with header crc but no payload protection
        UNPROTECTED(0),
        // uses our framing format with header crc24
        LZ4(1),
        // uses simple frames with separate header and payload crc
        CRC(2);

        public static Framing forId(int id)
        {
            switch (id)
            {
                case 0: return UNPROTECTED;
                case 1: return LZ4;
                case 2: return CRC;
            }
            throw new IllegalStateException();
        }

        final int id;
        Framing(int id)
        {
            this.id = id;
        }
    }

    public final IInternodeAuthenticator authenticator;
    public final InetAddressAndPort to;
    public final InetAddressAndPort connectTo; // may be represented by a different IP address on this node's local network
    public final ServerEncryptionOptions encryption;
    public final Framing framing;
    public final Integer socketSendBufferSizeInBytes;
    public final Integer applicationSendQueueCapacityInBytes;
    public final Integer applicationSendQueueReserveEndpointCapacityInBytes;
    public final ResourceLimits.Limit applicationSendQueueReserveGlobalCapacityInBytes;
    public final Boolean tcpNoDelay;
    public final int flushLowWaterMark, flushHighWaterMark;
    public final Integer tcpConnectTimeoutInMS;
    public final Integer tcpUserTimeoutInMS;
    public final AcceptVersions acceptVersions;
    public final InetAddressAndPort from;
    public final SocketFactory socketFactory;
    public final OutboundMessageCallbacks callbacks;
    public final OutboundDebugCallbacks debug;
    public final EndpointMessagingVersions endpointToVersion;

    public OutboundConnectionSettings(InetAddressAndPort to)
    {
        this(to, null);
    }

    public OutboundConnectionSettings(InetAddressAndPort to, InetAddressAndPort preferred)
    {
        this(null, to, preferred, null, null, null, null, null, null, null, 1 << 15, 1 << 16, null, null, null, null, null, null, null, null);
    }

    private OutboundConnectionSettings(IInternodeAuthenticator authenticator,
                                       InetAddressAndPort to,
                                       InetAddressAndPort connectTo,
                                       ServerEncryptionOptions encryption,
                                       Framing framing,
                                       Integer socketSendBufferSizeInBytes,
                                       Integer applicationSendQueueCapacityInBytes,
                                       Integer applicationSendQueueReserveEndpointCapacityInBytes,
                                       ResourceLimits.Limit applicationSendQueueReserveGlobalCapacityInBytes,
                                       Boolean tcpNoDelay,
                                       int flushLowWaterMark,
                                       int flushHighWaterMark,
                                       Integer tcpConnectTimeoutInMS,
                                       Integer tcpUserTimeoutInMS,
                                       AcceptVersions acceptVersions,
                                       InetAddressAndPort from,
                                       SocketFactory socketFactory,
                                       OutboundMessageCallbacks callbacks,
                                       OutboundDebugCallbacks debug,
                                       EndpointMessagingVersions endpointToVersion)
    {
        Preconditions.checkArgument(socketSendBufferSizeInBytes == null || socketSendBufferSizeInBytes == 0 || socketSendBufferSizeInBytes >= 1 << 10, "illegal socket send buffer size: " + socketSendBufferSizeInBytes);
        Preconditions.checkArgument(applicationSendQueueCapacityInBytes == null || applicationSendQueueCapacityInBytes >= 1 << 10, "illegal application send queue capacity: " + applicationSendQueueCapacityInBytes);
        Preconditions.checkArgument(tcpUserTimeoutInMS == null || tcpUserTimeoutInMS >= 0, "tcp user timeout must be non negative: " + tcpUserTimeoutInMS);
        Preconditions.checkArgument(tcpConnectTimeoutInMS == null || tcpConnectTimeoutInMS > 0, "tcp connect timeout must be positive: " + tcpConnectTimeoutInMS);
        Preconditions.checkArgument(acceptVersions == null || acceptVersions.min >= MessagingService.minimum_version, "acceptVersions.min must be minimum_version or higher: " + (acceptVersions == null ? null : acceptVersions.min));

        this.authenticator = authenticator;
        this.to = to;
        this.connectTo = connectTo;
        this.encryption = encryption;
        this.framing = framing;
        this.socketSendBufferSizeInBytes = socketSendBufferSizeInBytes;
        this.applicationSendQueueCapacityInBytes = applicationSendQueueCapacityInBytes;
        this.applicationSendQueueReserveEndpointCapacityInBytes = applicationSendQueueReserveEndpointCapacityInBytes;
        this.applicationSendQueueReserveGlobalCapacityInBytes = applicationSendQueueReserveGlobalCapacityInBytes;
        this.tcpNoDelay = tcpNoDelay;
        this.flushLowWaterMark = flushLowWaterMark;
        this.flushHighWaterMark = flushHighWaterMark;
        this.tcpConnectTimeoutInMS = tcpConnectTimeoutInMS;
        this.tcpUserTimeoutInMS = tcpUserTimeoutInMS;
        this.acceptVersions = acceptVersions;
        this.from = from;
        this.socketFactory = socketFactory;
        this.callbacks = callbacks;
        this.debug = debug;
        this.endpointToVersion = endpointToVersion;
    }

    public boolean withEncryption()
    {
        return encryption != null;
    }

    public String toString()
    {
        return String.format("peer: (%s, %s), framing: %s, encryption: %s",
                             to, connectTo, framing, SocketFactory.encryptionOptionsSummary(encryption));
    }

    public OutboundConnectionSettings withAuthenticator(IInternodeAuthenticator authenticator)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    @SuppressWarnings("unused")
    public OutboundConnectionSettings toEndpoint(InetAddressAndPort endpoint)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    public OutboundConnectionSettings withConnectTo(InetAddressAndPort connectTo)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    public OutboundConnectionSettings withEncryption(ServerEncryptionOptions encryption)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    @SuppressWarnings("unused")
    public OutboundConnectionSettings withFraming(Framing framing)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    public OutboundConnectionSettings withSocketSendBufferSizeInBytes(int socketSendBufferSizeInBytes)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    @SuppressWarnings("unused")
    public OutboundConnectionSettings withApplicationSendQueueCapacityInBytes(int applicationSendQueueCapacityInBytes)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    public OutboundConnectionSettings withApplicationReserveSendQueueCapacityInBytes(Integer applicationReserveSendQueueEndpointCapacityInBytes, ResourceLimits.Limit applicationReserveSendQueueGlobalCapacityInBytes)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    @SuppressWarnings("unused")
    public OutboundConnectionSettings withTcpNoDelay(boolean tcpNoDelay)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    @SuppressWarnings("unused")
    public OutboundConnectionSettings withNettyBufferBounds(WriteBufferWaterMark nettyBufferBounds)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    public OutboundConnectionSettings withTcpConnectTimeoutInMS(int tcpConnectTimeoutInMS)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    public OutboundConnectionSettings withTcpUserTimeoutInMS(int tcpUserTimeoutInMS)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    public OutboundConnectionSettings withAcceptVersions(AcceptVersions acceptVersions)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    public OutboundConnectionSettings withFrom(InetAddressAndPort from)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    public OutboundConnectionSettings withSocketFactory(SocketFactory socketFactory)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    public OutboundConnectionSettings withCallbacks(OutboundMessageCallbacks callbacks)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    public OutboundConnectionSettings withDebugCallbacks(OutboundDebugCallbacks debug)
    {
        return new OutboundConnectionSettings(authenticator, to, connectTo, encryption, framing,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationSendQueueReserveEndpointCapacityInBytes, applicationSendQueueReserveGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory, callbacks, debug, endpointToVersion);
    }

    public OutboundConnectionSettings withDefaultReserveLimits()
    {
        Integer applicationReserveSendQueueEndpointCapacityInBytes = this.applicationSendQueueReserveEndpointCapacityInBytes;
        ResourceLimits.Limit applicationReserveSendQueueGlobalCapacityInBytes = this.applicationSendQueueReserveGlobalCapacityInBytes;

        if (applicationReserveSendQueueEndpointCapacityInBytes == null)
            applicationReserveSendQueueEndpointCapacityInBytes = DatabaseDescriptor.getInternodeApplicationSendQueueReserveEndpointCapacityInBytes();
        if (applicationReserveSendQueueGlobalCapacityInBytes == null)
            applicationReserveSendQueueGlobalCapacityInBytes = MessagingService.instance().outboundGlobalReserveLimit;

        return withApplicationReserveSendQueueCapacityInBytes(applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes);
    }

    public IInternodeAuthenticator authenticator()
    {
        return authenticator != null ? authenticator : DatabaseDescriptor.getInternodeAuthenticator();
    }

    public EndpointMessagingVersions endpointToVersion()
    {
        if (endpointToVersion == null)
            return instance().versions;
        return endpointToVersion;
    }

    public InetAddressAndPort from()
    {
        return from != null ? from : FBUtilities.getBroadcastAddressAndPort();
    }

    public OutboundDebugCallbacks debug()
    {
        return debug != null ? debug : OutboundDebugCallbacks.NONE;
    }

    public ServerEncryptionOptions encryption()
    {
        return encryption != null ? encryption : defaultEncryptionOptions(to);
    }

    public SocketFactory socketFactory()
    {
        return socketFactory != null ? socketFactory : instance().socketFactory;
    }

    public OutboundMessageCallbacks callbacks()
    {
        return callbacks != null ? callbacks : instance().callbacks;
    }

    public int socketSendBufferSizeInBytes()
    {
        return socketSendBufferSizeInBytes != null ? socketSendBufferSizeInBytes
                                                   : DatabaseDescriptor.getInternodeSocketSendBufferSizeInBytes();
    }

    public int applicationSendQueueCapacityInBytes()
    {
        return applicationSendQueueCapacityInBytes != null ? applicationSendQueueCapacityInBytes
                                                           : DatabaseDescriptor.getInternodeApplicationSendQueueCapacityInBytes();
    }

    public ResourceLimits.Limit applicationSendQueueReserveGlobalCapacityInBytes()
    {
        return applicationSendQueueReserveGlobalCapacityInBytes != null ? applicationSendQueueReserveGlobalCapacityInBytes
                                                                        : instance().outboundGlobalReserveLimit;
    }

    public int applicationSendQueueReserveEndpointCapacityInBytes()
    {
        return applicationSendQueueReserveEndpointCapacityInBytes != null ? applicationSendQueueReserveEndpointCapacityInBytes
                                                                          : DatabaseDescriptor.getInternodeApplicationReceiveQueueReserveEndpointCapacityInBytes();
    }

    public int tcpConnectTimeoutInMS()
    {
        return tcpConnectTimeoutInMS != null ? tcpConnectTimeoutInMS
                                             : DatabaseDescriptor.getInternodeTcpConnectTimeoutInMS();
    }

    public int tcpUserTimeoutInMS(ConnectionCategory category)
    {
        // Reusing tcpUserTimeoutInMS for both messaging and streaming, since the connection is created for either one of them.
        if (tcpUserTimeoutInMS != null)
            return tcpUserTimeoutInMS;

        switch (category)
        {
            case MESSAGING: return DatabaseDescriptor.getInternodeTcpUserTimeoutInMS();
            case STREAMING: return DatabaseDescriptor.getInternodeStreamingTcpUserTimeoutInMS();
            default: throw new IllegalArgumentException("Unknown connection category: " + category);
        }
    }

    public boolean tcpNoDelay()
    {
        if (tcpNoDelay != null)
            return tcpNoDelay;

        if (DatabaseDescriptor.isClientOrToolInitialized() || isInLocalDC(getEndpointSnitch(), getBroadcastAddressAndPort(), to))
            return INTRADC_TCP_NODELAY;

        return DatabaseDescriptor.getInterDCTcpNoDelay();
    }

    public AcceptVersions acceptVersions(ConnectionCategory category)
    {
        return acceptVersions != null ? acceptVersions
                                      : category.isStreaming()
                                        ? MessagingService.accept_streaming
                                        : MessagingService.accept_messaging;
    }

    public InetAddressAndPort connectTo()
    {
        InetAddressAndPort connectTo = this.connectTo;
        if (connectTo == null)
            connectTo = SystemKeyspace.getPreferredIP(to);
        if (FBUtilities.getBroadcastAddressAndPort().equals(connectTo))
            return FBUtilities.getLocalAddressAndPort();
        return connectTo;
    }

    public String connectToId()
    {
        return !to.equals(connectTo())
             ? to.toString()
             : to.toString() + '(' + connectTo().toString() + ')';
    }

    public Framing framing(ConnectionCategory category)
    {
        if (framing != null)
            return framing;

        if (category.isStreaming())
            return Framing.UNPROTECTED;

        return shouldCompressConnection(getEndpointSnitch(), getBroadcastAddressAndPort(), to)
               ? Framing.LZ4 : Framing.CRC;
    }

    // note that connectTo is updated even if specified, in the case of pre40 messaging and using encryption (to update port)
    public OutboundConnectionSettings withDefaults(ConnectionCategory category)
    {
        if (to == null)
            throw new IllegalArgumentException();

        return new OutboundConnectionSettings(authenticator(), to, connectTo(),
                                              encryption(), framing(category),
                                              socketSendBufferSizeInBytes(), applicationSendQueueCapacityInBytes(),
                                              applicationSendQueueReserveEndpointCapacityInBytes(),
                                              applicationSendQueueReserveGlobalCapacityInBytes(),
                                              tcpNoDelay(), flushLowWaterMark, flushHighWaterMark,
                                              tcpConnectTimeoutInMS(), tcpUserTimeoutInMS(category), acceptVersions(category),
                                              from(), socketFactory(), callbacks(), debug(), endpointToVersion());
    }

    private static boolean isInLocalDC(IEndpointSnitch snitch, InetAddressAndPort localHost, InetAddressAndPort remoteHost)
    {
        String remoteDC = snitch.getDatacenter(remoteHost);
        String localDC = snitch.getDatacenter(localHost);
        return remoteDC != null && remoteDC.equals(localDC);
    }

    @VisibleForTesting
    static ServerEncryptionOptions defaultEncryptionOptions(InetAddressAndPort endpoint)
    {
        ServerEncryptionOptions options = DatabaseDescriptor.getInternodeMessagingEncyptionOptions();
        return options.shouldEncrypt(endpoint) ? options : null;
    }

    @VisibleForTesting
    static boolean shouldCompressConnection(IEndpointSnitch snitch, InetAddressAndPort localHost, InetAddressAndPort remoteHost)
    {
        return (DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.all)
               || ((DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.dc) && !isInLocalDC(snitch, localHost, remoteHost));
    }

}
