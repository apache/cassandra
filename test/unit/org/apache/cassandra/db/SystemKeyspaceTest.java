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
package org.apache.cassandra.db;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.SystemKeyspace.LOCAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SystemKeyspaceTest
{
    @BeforeClass
    public static void prepSnapshotTracker()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @Test
    public void testLocalTokens()
    {
        // Remove all existing tokens
        Collection<Token> current = SystemKeyspace.loadTokens().asMap().get(FBUtilities.getLocalAddressAndPort());
        if (current != null && !current.isEmpty())
            SystemKeyspace.updateTokens(current);

        List<Token> tokens = new ArrayList<Token>()
        {{
            for (int i = 0; i < 9; i++)
                add(new BytesToken(ByteBufferUtil.bytes(String.format("token%d", i))));
        }};

        SystemKeyspace.updateTokens(tokens);
        int count = 0;

        for (Token tok : SystemKeyspace.getSavedTokens())
            assert tokens.get(count++).equals(tok);
    }

    @Test
    public void testNonLocalToken() throws UnknownHostException
    {
        BytesToken token = new BytesToken(ByteBufferUtil.bytes("token3"));
        InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.2");
        SystemKeyspace.updateTokens(address, Collections.<Token>singletonList(token));
        assert SystemKeyspace.loadTokens().get(address).contains(token);
        SystemKeyspace.removeEndpoint(address);
        assert !SystemKeyspace.loadTokens().containsValue(token);
    }

    @Test
    public void testLocalHostID()
    {
        UUID firstId = SystemKeyspace.getOrInitializeLocalHostId();
        UUID secondId = SystemKeyspace.getOrInitializeLocalHostId();
        assert firstId.equals(secondId) : String.format("%s != %s%n", firstId.toString(), secondId.toString());
    }

    private void assertDeleted()
    {
        assertTrue(getSystemSnapshotFiles(SchemaConstants.SYSTEM_KEYSPACE_NAME).isEmpty());
    }

    @Test
    public void snapshotSystemKeyspaceIfUpgrading() throws IOException
    {
        // First, check that in the absence of any previous installed version, we don't create snapshots
        for (ColumnFamilyStore cfs : Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStores())
            cfs.clearUnsafe();
        StorageService.instance.clearSnapshot(Collections.emptyMap(), null, SchemaConstants.SYSTEM_KEYSPACE_NAME);

        SystemKeyspace.snapshotOnVersionChange();
        assertDeleted();

        // now setup system.local as if we're upgrading from a previous version
        setupReleaseVersion(getOlderVersionString());
        StorageService.instance.clearSnapshot(Collections.emptyMap(), null, SchemaConstants.SYSTEM_KEYSPACE_NAME);
        assertDeleted();

        // Compare versions again & verify that snapshots were created for all tables in the system ks
        SystemKeyspace.snapshotOnVersionChange();

        Set<String> snapshottedSystemTables = getSystemSnapshotFiles(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        SystemKeyspace.metadata().tables.forEach(t -> assertTrue(snapshottedSystemTables.contains(t.name)));
        Set<String> snapshottedSchemaTables = getSystemSnapshotFiles(SchemaConstants.SCHEMA_KEYSPACE_NAME);
        SchemaKeyspace.metadata().tables.forEach(t -> assertTrue(snapshottedSchemaTables.contains(t.name)));

        // clear out the snapshots & set the previous recorded version equal to the latest, we shouldn't
        // see any new snapshots created this time.
        StorageService.instance.clearSnapshot(Collections.emptyMap(), null, SchemaConstants.SYSTEM_KEYSPACE_NAME);
        setupReleaseVersion(FBUtilities.getReleaseVersionString());

        SystemKeyspace.snapshotOnVersionChange();

        // snapshotOnVersionChange for upgrade case will open a SSTR when the CFS is flushed.
        // 10 files expected.
        assertDeleted();

        StorageService.instance.clearSnapshot(Collections.emptyMap(), null, SchemaConstants.SYSTEM_KEYSPACE_NAME);
    }

    @Test
    public void testPersistLocalMetadata()
    {
        SystemKeyspace.persistLocalMetadata();

        UntypedResultSet result = executeInternal(format("SELECT * FROM system.%s WHERE key='%s'", LOCAL, LOCAL));

        assertNotNull(result);
        UntypedResultSet.Row row = result.one();

        assertEquals(DatabaseDescriptor.getClusterName(), row.getString("cluster_name"));
        assertEquals(FBUtilities.getReleaseVersionString(), row.getString("release_version"));
        assertEquals(QueryProcessor.CQL_VERSION.toString(), row.getString("cql_version"));
        assertEquals(String.valueOf(ProtocolVersion.CURRENT.asInt()), row.getString("native_protocol_version"));
        assertEquals(DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter(), row.getString("data_center"));
        assertEquals(DatabaseDescriptor.getEndpointSnitch().getLocalRack(), row.getString("rack"));
        assertEquals(DatabaseDescriptor.getPartitioner().getClass().getName(), row.getString("partitioner"));
        assertEquals(FBUtilities.getJustBroadcastNativeAddress(), row.getInetAddress("rpc_address"));
        assertEquals(DatabaseDescriptor.getNativeTransportPort(), row.getInt("rpc_port"));
        assertEquals(FBUtilities.getJustBroadcastAddress(), row.getInetAddress("broadcast_address"));
        assertEquals(DatabaseDescriptor.getStoragePort(), row.getInt("broadcast_port"));
        assertEquals(FBUtilities.getJustLocalAddress(), row.getInetAddress("listen_address"));
        assertEquals(DatabaseDescriptor.getStoragePort(), row.getInt("listen_port"));
    }

    private String getOlderVersionString()
    {
        String version = FBUtilities.getReleaseVersionString();
        CassandraVersion semver = new CassandraVersion(version.contains("-") ? version.substring(0, version.indexOf('-'))
                                                                           : version);
        return (String.format("%s.%s.%s", semver.major - 1, semver.minor, semver.patch));
    }

    private Set<String> getSystemSnapshotFiles(String keyspace)
    {
        Set<String> snapshottedTableNames = new HashSet<>();
        for (ColumnFamilyStore cfs : Keyspace.open(keyspace).getColumnFamilyStores())
        {
            if (!cfs.listSnapshots().isEmpty())
                snapshottedTableNames.add(cfs.getTableName());
        }
        return snapshottedTableNames;
    }

    private void setupReleaseVersion(String version)
    {
        // besides the release_version, we also need to insert the cluster_name or the check
        // in SystemKeyspace.checkHealth were we verify it matches DatabaseDescriptor will fail
        QueryProcessor.executeInternal(String.format("INSERT INTO system.local(key, release_version, cluster_name) " +
                                                     "VALUES ('local', '%s', '%s')",
                                                     version,
                                                     DatabaseDescriptor.getClusterName()));
        String r = readLocalVersion();
        assertEquals(String.format("Expected %s, got %s", version, r), version, r);
    }

    private String readLocalVersion()
    {
        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT release_version FROM system.local WHERE key='local'");
        return rs.isEmpty() || !rs.one().has("release_version") ? null : rs.one().getString("release_version");
    }
}
