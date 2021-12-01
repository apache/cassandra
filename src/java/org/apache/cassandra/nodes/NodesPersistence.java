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

package org.apache.cassandra.nodes;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Throwables;

import static java.lang.String.format;
import static org.apache.cassandra.db.SystemKeyspace.LEGACY_PEERS;
import static org.apache.cassandra.db.SystemKeyspace.LOCAL;
import static org.apache.cassandra.db.SystemKeyspace.PEERS_V2;
import static org.apache.cassandra.db.SystemKeyspace.forceBlockingFlush;

public class NodesPersistence implements INodesPersistence
{
    private static final Logger logger = LoggerFactory.getLogger(NodesPersistence.class);

    private static final List<String> COMMON_COLUMNS = ImmutableList.of("data_center",
                                                                        "rack",
                                                                        "host_id",
                                                                        "release_version",
                                                                        "schema_version",
                                                                        "tokens");

    private static final String INSERT_LOCAL_STMT = format("INSERT INTO system.%s (" +
                                                           "     key, " +
                                                           "     bootstrapped, " +
                                                           "     broadcast_address, " +
                                                           "     broadcast_port, " +
                                                           "     cluster_name, " +
                                                           "     cql_version, " +
                                                           "     listen_address, " +
                                                           "     listen_port, " +
                                                           "     native_protocol_version, " +
                                                           "     partitioner, " +
                                                           "     rpc_address, " +
                                                           "     rpc_port, " +
                                                           "     truncated_at, " +
                                                           "     %s) " +
                                                           "VALUES (%s)", LOCAL,
                                                           StringUtils.join(COMMON_COLUMNS, ", "),
                                                           StringUtils.repeat("?", ", ", 13 + COMMON_COLUMNS.size()));

    private static final String INSERT_PEER_STMT = format("INSERT INTO system.%s (" +
                                                          "     peer, " +
                                                          "     peer_port, " +
                                                          "     preferred_ip, " +
                                                          "     preferred_port, " +
                                                          "     native_address, " +
                                                          "     native_port, " +
                                                          "     %s) " +
                                                          "VALUES (%s)", PEERS_V2,
                                                          StringUtils.join(COMMON_COLUMNS, ", "),
                                                          StringUtils.repeat("?", ", ", 6 + COMMON_COLUMNS.size()));

    private static final String INSERT_LEGACY_PEER_STMT = format("INSERT INTO system.%s (" +
                                                                 "     peer, " +
                                                                 "     preferred_ip, " +
                                                                 "     rpc_address, " +
                                                                 "     %s) " +
                                                                 "VALUES (%s)", LEGACY_PEERS,
                                                                 StringUtils.join(COMMON_COLUMNS, ", "),
                                                                 StringUtils.repeat("?", ", ", 3 + COMMON_COLUMNS.size()));

    private static final String DELETE_PEER_STMT = format("DELETE FROM system.%s " +
                                                          "WHERE peer = ? AND peer_port = ?", PEERS_V2);

    private static final String DELETE_LEGACY_PEER_STMT = format("DELETE FROM system.%s " +
                                                                 "WHERE peer = ?", LEGACY_PEERS);


    @Override
    public LocalInfo loadLocal()
    {
        UntypedResultSet results = QueryProcessor.executeInternal("SELECT * FROM system." + LOCAL + " WHERE key = ?", LOCAL);
        if (results == null || results.isEmpty())
            return null;
        Row row = results.one();
        LocalInfo info = readCommonInfo(new LocalInfo(), row);
        info.setBroadcastAddressAndPort(readInetAddressAndPort(row, "broadcast_address", "broadcast_port", DatabaseDescriptor.getStoragePort()))
            .setListenAddressAndPort(readInetAddressAndPort(row, "listen_address", "listen_port", DatabaseDescriptor.getStoragePort()))
            .setNativeTransportAddressAndPort(readInetAddressAndPort(row, "rpc_address", "rpc_port", DatabaseDescriptor.getNativeTransportPort()))
            .setBootstrapState(readBootstrapState(row, "bootstrapped"))
            .setClusterName(row.has("cluster_name") ? row.getString("cluster_name") : null)
            .setCqlVersion(readCassandraVersion(row, "cql_version"))
            .setNativeProtocolVersion(readNativeProtocol(row, "native_protocol_version"))
            .setPartitionerClass(readPartitionerClass(row, "partitioner"))
            .setTruncationRecords(readTruncationRecords(row, "truncated_at"));
        return info;
    }

    @Override
    public void saveLocal(LocalInfo info)
    {
        Object[] values = ArrayUtils.addAll(new Object[]{ "local",
                                                          info.getBootstrapState() != null ? info.getBootstrapState().name() : null,
                                                          serializeAddress(info.getBroadcastAddressAndPort()),
                                                          serializePort(info.getBroadcastAddressAndPort()),
                                                          info.getClusterName(),
                                                          serializeCassandraVersion(info.getCqlVersion()),
                                                          serializeAddress(info.getListenAddressAndPort()),
                                                          serializePort(info.getListenAddressAndPort()),
                                                          serializeProtocolVersion(info.getNativeProtocolVersion()),
                                                          info.getPartitionerClass() != null ? info.getPartitionerClass().getName() : null,
                                                          serializeAddress(info.getNativeTransportAddressAndPort()),
                                                          serializePort(info.getNativeTransportAddressAndPort()),
                                                          serializeTruncationRecords(info.getTruncationRecords()) }, serializeCommonInfo(info));

        QueryProcessor.executeInternal(INSERT_LOCAL_STMT, values);
    }

    private String serializeProtocolVersion(ProtocolVersion protocolVersion)
    {
        return protocolVersion == null ? null : String.valueOf(protocolVersion.asInt());
    }

    @Override
    public void syncLocal()
    {
        try
        {
            forceBlockingFlush(LOCAL);
        }
        catch (RejectedExecutionException ex)
        {
            logger.warn("Could not flush peers table because the thread pool has shut down", ex);
        }
    }

    @Override
    public Stream<PeerInfo> loadPeers()
    {
        UntypedResultSet results = QueryProcessor.executeInternal("SELECT * FROM system." + PEERS_V2);
        return StreamSupport.stream(results.spliterator(), false).map(row -> {
            PeerInfo info = readCommonInfo(new PeerInfo(), row);
            info.setPeerAddressAndPort(readInetAddressAndPort(row, "peer", "peer_port", DatabaseDescriptor.getStoragePort()))
                .setPreferredAddressAndPort(readInetAddressAndPort(row, "preferred_ip", "preferred_port", DatabaseDescriptor.getStoragePort()))
                .setNativeTransportAddressAndPort(readInetAddressAndPort(row, "native_address", "native_port", DatabaseDescriptor.getNativeTransportPort()));
            return info;
        });
    }

    @Override
    public void savePeer(PeerInfo info)
    {
        Object[] peersValues = ArrayUtils.addAll(new Object[]{ serializeAddress(info.getPeerAddressAndPort()),
                                                               serializePort(info.getPeerAddressAndPort()),
                                                               serializeAddress(info.getPreferredAddressAndPort()),
                                                               serializePort(info.getPreferredAddressAndPort()),
                                                               serializeAddress(info.getNativeTransportAddressAndPort()),
                                                               serializePort(info.getNativeTransportAddressAndPort()),
                                                               }, serializeCommonInfo(info));
        QueryProcessor.executeInternal(INSERT_PEER_STMT, peersValues);

        Object[] legacyPeersValues = ArrayUtils.addAll(new Object[]{ serializeAddress(info.getPeerAddressAndPort()),
                                                                     serializeAddress(info.getPreferredAddressAndPort()),
                                                                     serializeAddress(info.getNativeTransportAddressAndPort()),
                                                                     }, serializeCommonInfo(info));

        QueryProcessor.executeInternal(INSERT_LEGACY_PEER_STMT, legacyPeersValues);
    }

    @Override
    public void deletePeer(InetAddressAndPort endpoint)
    {
        QueryProcessor.executeInternal(DELETE_PEER_STMT, serializeAddress(endpoint), serializePort(endpoint));
        QueryProcessor.executeInternal(DELETE_LEGACY_PEER_STMT, serializeAddress(endpoint));
    }

    @Override
    public void syncPeers()
    {
        try
        {
            forceBlockingFlush(LEGACY_PEERS, PEERS_V2);
        }
        catch (RejectedExecutionException ex)
        {
            logger.warn("Could not flush peers table because the thread pool has shut down", ex);
        }
    }

    private <T extends NodeInfo<T>> T readCommonInfo(T info, Row row)
    {
        info.setDataCenter(row.has("data_center") ? row.getString("data_center") : null)
            .setRack(row.has("rack") ? row.getString("rack") : null)
            .setHostId(row.has("host_id") ? row.getUUID("host_id") : null)
            .setReleaseVersion(readCassandraVersion(row, "release_version"))
            .setSchemaVersion(row.has("schema_version") ? row.getUUID("schema_version") : null)
            .setTokens(readTokens(row, "tokens"));
        return info;
    }

    private InetAddressAndPort readInetAddressAndPort(Row row, String addressCol, String portCol, int defaultPort)
    {
        InetAddress address = row.has(addressCol) ? row.getInetAddress(addressCol) : null;
        if (address == null)
            return null;
        int port = row.has(portCol) ? row.getInt(portCol) : defaultPort;
        return InetAddressAndPort.getByAddressOverrideDefaults(address, port);
    }

    private CassandraVersion readCassandraVersion(Row row, String col)
    {
        String v = row.has(col) ? row.getString(col) : null;
        if (v == null)
            return null;
        return new CassandraVersion(v);
    }

    private Collection<Token> readTokens(Row row, String col)
    {
        Set<String> tokensStrings = row.has(col) ? row.getSet(col, UTF8Type.instance) : new HashSet<>();
        Token.TokenFactory factory = StorageService.instance.getTokenFactory();
        List<Token> tokens = new ArrayList<>(tokensStrings.size());
        for (String tk : tokensStrings)
            tokens.add(factory.fromString(tk));
        return tokens;
    }

    private SystemKeyspace.BootstrapState readBootstrapState(Row row, String col)
    {
        String s = row.has(col) ? row.getString(col) : null;
        if (s == null)
            return null;

        return SystemKeyspace.BootstrapState.valueOf(s);
    }

    @SuppressWarnings("unchecked")
    private Class<? extends IPartitioner> readPartitionerClass(Row row, String col)
    {
        String s = row.has(col) ? row.getString(col) : null;
        if (s == null)
            return null;

        try
        {
            return (Class<? extends IPartitioner>) Class.forName(s);
        }
        catch (ClassNotFoundException e)
        {
            throw Throwables.unchecked(e);
        }
    }

    private ProtocolVersion readNativeProtocol(Row row, String col)
    {
        String s = row.has(col) ? row.getString(col) : null;
        if (s == null)
            return null;

        return ProtocolVersion.decode(Integer.parseInt(s), true);
    }

    private Map<UUID, TruncationRecord> readTruncationRecords(Row row, String col)
    {
        Map<UUID, ByteBuffer> raw = row.has(col) ? row.getMap(col, UUIDType.instance, BytesType.instance) : ImmutableMap.of();
        if (raw == null)
            return null;

        return raw.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> truncationRecordFromBlob(e.getValue())));
    }

    private TruncationRecord truncationRecordFromBlob(ByteBuffer bytes)
    {
        try (RebufferingInputStream in = new DataInputBuffer(bytes, true))
        {
            return new TruncationRecord(CommitLogPosition.serializer.deserialize(in), in.available() > 0 ? in.readLong() : Long.MIN_VALUE);
        }
        catch (IOException e)
        {
            throw Throwables.unchecked(e);
        }
    }

    public static int serializePort(InetAddressAndPort addressAndPort)
    {
        return addressAndPort != null ? addressAndPort.port : -1;
    }

    public static InetAddress serializeAddress(InetAddressAndPort addressAndPort)
    {
        return addressAndPort != null ? addressAndPort.address : null;
    }

    public static String serializeCassandraVersion(CassandraVersion version)
    {
        return version != null ? version.toString() : null;
    }

    public static Set<String> serializeTokens(Collection<Token> tokens)
    {
        if (tokens.isEmpty())
            return Collections.emptySet();
        Token.TokenFactory factory = StorageService.instance.getTokenFactory();
        Set<String> s = new HashSet<>(tokens.size());
        for (Token tk : tokens)
            s.add(factory.toString(tk));
        return s;
    }

    private Object[] serializeCommonInfo(NodeInfo<?> info)
    {
        return new Object[]{ info.getDataCenter(),
                             info.getRack(),
                             info.getHostId(),
                             serializeCassandraVersion(info.getReleaseVersion()),
                             info.getSchemaVersion(),
                             serializeTokens(info.getTokens()) };
    }

    public static ByteBuffer serializeTruncationRecord(TruncationRecord truncationRecord)
    {
        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            CommitLogPosition.serializer.serialize(truncationRecord.position, out);
            out.writeLong(truncationRecord.truncatedAt);
            return out.asNewBuffer();
        }
        catch (IOException e)
        {
            throw Throwables.unchecked(e);
        }
    }

    public static Map<UUID, ByteBuffer> serializeTruncationRecords(Map<UUID, TruncationRecord> truncationRecords)
    {
        return truncationRecords.entrySet()
                                .stream()
                                .collect(Collectors.toMap(Map.Entry::getKey, e -> serializeTruncationRecord(e.getValue())));
    }
}
