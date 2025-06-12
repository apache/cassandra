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

package org.apache.cassandra.transport;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.Set;

import org.apache.cassandra.metrics.ClientMetricsManager;
import org.apache.cassandra.db.monitoring.BadQuery;
import org.apache.cassandra.transport.ClientResourceLimits.Overload;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.VoidChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.Attribute;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.net.AsyncChannelPromise;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.transport.messages.SupportedMessage;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.config.Config.ClientLibsEnforcementLevel;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Added to the Netty pipeline whenever a new Channel is initialized. This handler only processes
 * the messages which constitute the initial handshake between client and server, namely
 * OPTIONS and STARTUP. After receiving a STARTUP message, the pipeline is reconfigured according
 * to the protocol version which was negotiated. That reconfiguration should include removing this
 * handler from the pipeline.
 */
public class InitialConnectionHandler extends ByteToMessageDecoder
{
    private static final Logger logger = LoggerFactory.getLogger(InitialConnectionHandler.class);

    private static final String CLIENT_IP_PORT = "CLIENT_IP_PORT";
    private static final String CLIENT_PREFIX = "CLIENT_";
    private static final String DRIVER_NAME = "DRIVER_NAME";
    private static final String HOST_NAME = "HOST_NAME";
    private static final String IS_AUTHENTICATED = "IS_AUTHENTICATED";
    private static final String LANGUAGE = "LANGUAGE";
    private static final String REQUEST_TENANCY = "REQUEST_TENANCY";
    private static final String SERVICE = "SERVICE";
    private static final String TIER = "TIER";
    private static final String UNDEFINED = "undefined";
    private static final String CLIENT_LIBS_ENFORCEMENT_LEVEL = "ClientLibsEnforcementLevel";

    final Envelope.Decoder decoder;
    final Connection.Factory factory;
    final PipelineConfigurator configurator;

    InitialConnectionHandler(Envelope.Decoder decoder, Connection.Factory factory, PipelineConfigurator configurator)
    {
        this.decoder = decoder;
        this.factory = factory;
        this.configurator = configurator;
    }

    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> list) throws Exception
    {
        Envelope inbound = decoder.decode(buffer);
        if (inbound == null)
            return;

        try
        {
            Envelope outbound;
            switch (inbound.header.type)
            {
                case OPTIONS:
                    logger.trace("OPTIONS received {}", inbound.header.version);
                    List<String> cqlVersions = new ArrayList<>();
                    cqlVersions.add(QueryProcessor.CQL_VERSION.toString());

                    List<String> compressions = new ArrayList<>();
                    if (Compressor.SnappyCompressor.instance != null)
                        compressions.add("snappy");
                    // LZ4 is always available since worst case scenario it default to a pure JAVA implem.
                    compressions.add("lz4");

                    Map<String, List<String>> supportedOptions = new HashMap<>();
                    supportedOptions.put(StartupMessage.CQL_VERSION, cqlVersions);
                    supportedOptions.put(StartupMessage.COMPRESSION, compressions);
                    supportedOptions.put(StartupMessage.PROTOCOL_VERSIONS, ProtocolVersion.supportedVersions());
                    SupportedMessage supported = new SupportedMessage(supportedOptions);
                    supported.setStreamId(inbound.header.streamId);
                    outbound = supported.encode(inbound.header.version);
                    ctx.writeAndFlush(outbound);
                    break;

                case STARTUP:
                    Attribute<Connection> attrConn = ctx.channel().attr(Connection.attributeKey);
                    Connection connection = attrConn.get();
                    if (connection == null)
                    {
                        connection = factory.newConnection(ctx.channel(), inbound.header.version);
                        attrConn.set(connection);
                    }
                    assert connection instanceof ServerConnection;
                    ServerConnection serverConnection = (ServerConnection) connection;

                    StartupMessage startup = (StartupMessage) Message.Decoder.decodeMessage(ctx.channel(), inbound);
                    InetAddress remoteAddress = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress();

                    StringBuilder errorMsg = new StringBuilder();
                    if (!validateAndLogClientDetails(startup.options, (ServerConnection) connection, errorMsg)) {
                        ErrorMessage error = ErrorMessage.fromException(new ServerError(errorMsg.toString()));
                        outbound = error.encode(inbound.header.version);
                        ctx.writeAndFlush(outbound);
                        if (ctx.channel().isOpen())
                            ctx.channel().close();
                        return;
                    }

                    final ClientResourceLimits.Allocator allocator = ClientResourceLimits.getAllocatorForEndpoint(remoteAddress);
                    ChannelPromise promise;
                    if (inbound.header.version.isGreaterOrEqualTo(ProtocolVersion.V5))
                    {
                        // in this case we need to defer configuring the pipeline until after the response
                        // has been sent, as the frame encoding specified in v5 should not be applied to
                        // the STARTUP response.
                        allocator.allocate(inbound.header.bodySizeInBytes);
                        promise = AsyncChannelPromise.withListener(ctx, future -> {
                            if (future.isSuccess())
                            {
                                logger.trace("Response to STARTUP sent, configuring pipeline for {}", inbound.header.version);
                                configurator.configureModernPipeline(ctx, serverConnection, allocator, inbound.header.version, startup.options);
                                allocator.release(inbound.header.bodySizeInBytes);
                            }
                            else
                            {
                                Throwable cause = future.cause();
                                if (null == cause)
                                    cause = new ServerError("Unexpected error establishing connection");
                                logger.warn("Writing response to STARTUP failed, unable to configure pipeline", cause);
                                ErrorMessage error = ErrorMessage.fromException(cause);
                                Envelope response = error.encode(inbound.header.version);
                                ChannelPromise closeChannel = AsyncChannelPromise.withListener(ctx, f -> ctx.close());
                                ctx.writeAndFlush(response, closeChannel);
                                if (ctx.channel().isOpen())
                                    ctx.channel().close();
                            }
                        });
                    }
                    else
                    {
                        // no need to configure the pipeline asynchronously in this case
                        // the capacity obtained from allocator for the STARTUP message
                        // is released when flushed by the legacy dispatcher/flusher so
                        // there's no need to explicitly release that here either.
                        configurator.configureLegacyPipeline(ctx, allocator);
                        promise = new VoidChannelPromise(ctx.channel(), false);
                    }

                    final Message.Response response = Dispatcher.processRequest(ctx.channel(), startup, Overload.NONE, Dispatcher.RequestTime.forImmediateExecution());

                    outbound = response.encode(inbound.header.version);
                    ctx.writeAndFlush(outbound, promise);
                    logger.trace("Configured pipeline: {}", ctx.pipeline());
                    break;

                default:
                    ErrorMessage error =
                        ErrorMessage.fromException(
                            new ProtocolException(String.format("Unexpected message %s, expecting STARTUP or OPTIONS",
                                                                inbound.header.type)));
                    outbound = error.encode(inbound.header.version);
                    ctx.writeAndFlush(outbound);
            }
        }
        finally
        {
            inbound.release();
        }
    }

    /*
     * Validate and log the client details.
     */
    private boolean validateAndLogClientDetails(Map<String, String> options, ServerConnection connection, StringBuilder errorMsg)
    {
        Map<String, String> optionsWithClientPrefix = options.entrySet()
                                                        .stream()
                                                        .collect(Collectors.toMap(
                                                        entry -> CLIENT_PREFIX + entry.getKey(),
                                                        Map.Entry::getValue
                                                        ));

        InetSocketAddress clientSocketAddress = connection.getClientState().getRemoteAddress();
        if (clientSocketAddress != null) {
            String clientIPPort = String.format("%s_%s", clientSocketAddress.getAddress().getHostAddress(), clientSocketAddress.getPort());
            optionsWithClientPrefix.put(CLIENT_IP_PORT, clientIPPort);
        }

        String service = options.getOrDefault(SERVICE, "");
        String driverName = options.getOrDefault(DRIVER_NAME, options.getOrDefault(LANGUAGE, UNDEFINED)).toLowerCase().replaceAll("[^a-z0-9]+", "-");
        String key = service + "," + options.getOrDefault(REQUEST_TENANCY, "");

        // Validate client library driver if enforcement is enabled
        ClientLibsEnforcementLevel enforcementLevel = DatabaseDescriptor.getClientLibsEnforcementLevel();
        String enforcementLevelString = enforcementLevel.toString();
        Set<String> allowedDrivers = DatabaseDescriptor.getAllowedClientLibDrivers();
        boolean isDriverSupported = false;
        boolean isDriverAllowed = true;

        if (enforcementLevel == ClientLibsEnforcementLevel.none ||
            allowedDrivers.isEmpty() ||
            allowedDrivers.stream().anyMatch(pattern -> driverName.matches(pattern)))
        {
            isDriverSupported = true;
        }

        if (!isDriverSupported)
        {
            String nodeAddress = "unknown";
            try {
                nodeAddress = FBUtilities.getBroadcastNativeAddressAndPort().getHostAddress(true);
            } catch (Exception e) {
                logger.error("Error getting local host address", e);
            }

            errorMsg.append(String.format("Client driver '%s' is not in the allowed list. cassandra node = %s",
                            driverName,
                            nodeAddress));

            if (enforcementLevel == ClientLibsEnforcementLevel.hard) {
                logger.error("Hard enforcement: Rejecting connection: {}. Allowed drivers: {}, {}", errorMsg, allowedDrivers, optionsWithClientPrefix);
                isDriverAllowed = false;
            }
            else if (enforcementLevel == ClientLibsEnforcementLevel.soft) {
                NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, key + CLIENT_LIBS_ENFORCEMENT_LEVEL, 5, TimeUnit.MINUTES, "Soft enforcement: Allowing connection with unsupported driver: {}. Allowed drivers: {}", errorMsg, allowedDrivers);
                isDriverAllowed = true;
            } else { // enforcementLevel == ClientLibsEnforcementLevel.none
                isDriverAllowed = true;
            }
        }

        // Logging the client context data with NoSpamLogger
        NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, key, 5, TimeUnit.MINUTES, "Client context data: {}", optionsWithClientPrefix);
        // Send the client session metric
        ClientMetricsManager.getSessionMetrics(
            service,
            options.getOrDefault(REQUEST_TENANCY, ""),
            options.getOrDefault(TIER, "empty"),
            driverName,
            enforcementLevelString,
            String.valueOf(isDriverSupported),
            options.getOrDefault(IS_AUTHENTICATED, "false")).sessions.mark();

        // Check for service vs cassandra tier mismatch
        BadQuery.checkForTierMismatch(
            options.getOrDefault(TIER, "-1"),
            service
        );

        return isDriverAllowed;
    }
}
