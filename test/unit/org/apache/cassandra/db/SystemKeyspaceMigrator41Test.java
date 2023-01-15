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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.FBUtilities.now;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;

public class SystemKeyspaceMigrator41Test extends CQLTester
{
    @Test
    public void testMigratePeers() throws Throwable
    {
        String legacyTab = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_PEERS);
        String tab = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PEERS_V2);
        String insert = String.format("INSERT INTO %s ("
                                      + "peer, "
                                      + "data_center, "
                                      + "host_id, "
                                      + "preferred_ip, "
                                      + "rack, "
                                      + "release_version, "
                                      + "rpc_address, "
                                      + "schema_version, "
                                      + "tokens) "
                                      + " values ( ?, ?, ? , ? , ?, ?, ?, ?, ?)",
                                      legacyTab);
        UUID hostId = UUID.randomUUID();
        UUID schemaVersion = UUID.randomUUID();
        execute(insert,
                InetAddress.getByName("127.0.0.1"),
                "dcFoo",
                hostId,
                InetAddress.getByName("127.0.0.2"),
                "rackFoo", "4.0",
                InetAddress.getByName("127.0.0.3"),
                schemaVersion,
                ImmutableSet.of("foobar"));
        SystemKeyspaceMigrator41.migratePeers();

        int rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", tab)))
        {
            rowCount++;
            assertEquals(InetAddress.getByName("127.0.0.1"), row.getInetAddress("peer"));
            assertEquals(DatabaseDescriptor.getStoragePort(), row.getInt("peer_port"));
            assertEquals("dcFoo", row.getString("data_center"));
            assertEquals(hostId, row.getUUID("host_id"));
            assertEquals(InetAddress.getByName("127.0.0.2"), row.getInetAddress("preferred_ip"));
            assertEquals(DatabaseDescriptor.getStoragePort(), row.getInt("preferred_port"));
            assertEquals("rackFoo", row.getString("rack"));
            assertEquals("4.0", row.getString("release_version"));
            assertEquals(InetAddress.getByName("127.0.0.3"), row.getInetAddress("native_address"));
            assertEquals(DatabaseDescriptor.getNativeTransportPort(), row.getInt("native_port"));
            assertEquals(schemaVersion, row.getUUID("schema_version"));
            assertEquals(ImmutableSet.of("foobar"), row.getSet("tokens", UTF8Type.instance));
        }
        assertEquals(1, rowCount);

        //Test nulls/missing don't prevent the row from propagating
        execute(String.format("TRUNCATE %s", legacyTab));
        execute(String.format("TRUNCATE %s", tab));

        execute(String.format("INSERT INTO %s (peer) VALUES (?)", legacyTab),
                              InetAddress.getByName("127.0.0.1"));
        SystemKeyspaceMigrator41.migratePeers();

        rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", tab)))
        {
            rowCount++;
        }
        assertEquals(1, rowCount);
    }

    @Test
    public void testMigratePeerEvents() throws Throwable
    {
        String legacyTab = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_PEER_EVENTS);
        String tab = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PEER_EVENTS_V2);
        String insert = String.format("INSERT INTO %s ("
                                      + "peer, "
                                      + "hints_dropped) "
                                      + " values ( ?, ? )",
                                      legacyTab);
        TimeUUID uuid = nextTimeUUID();
        execute(insert,
                InetAddress.getByName("127.0.0.1"),
                ImmutableMap.of(uuid, 42));
        SystemKeyspaceMigrator41.migratePeerEvents();

        int rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", tab)))
        {
            rowCount++;
            assertEquals(InetAddress.getByName("127.0.0.1"), row.getInetAddress("peer"));
            assertEquals(DatabaseDescriptor.getStoragePort(), row.getInt("peer_port"));
            assertEquals(ImmutableMap.of(uuid, 42), row.getMap("hints_dropped", TimeUUIDType.instance, Int32Type.instance));
        }
        assertEquals(1, rowCount);

        //Test nulls/missing don't prevent the row from propagating
        execute(String.format("TRUNCATE %s", legacyTab));
        execute(String.format("TRUNCATE %s", tab));

        execute(String.format("INSERT INTO %s (peer) VALUES (?)", legacyTab),
                InetAddress.getByName("127.0.0.1"));
        SystemKeyspaceMigrator41.migratePeerEvents();

        rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", tab)))
        {
            rowCount++;
        }
        assertEquals(1, rowCount);
    }

    @Test
    public void testMigrateTransferredRanges() throws Throwable
    {
        String legacyTab = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_TRANSFERRED_RANGES);
        String tab = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.TRANSFERRED_RANGES_V2);
        String insert = String.format("INSERT INTO %s ("
                                      + "operation, "
                                      + "peer, "
                                      + "keyspace_name, "
                                      + "ranges) "
                                      + " values ( ?, ?, ?, ? )",
                                      legacyTab);
        execute(insert,
                "foo",
                InetAddress.getByName("127.0.0.1"),
                "bar",
                ImmutableSet.of(ByteBuffer.wrap(new byte[] { 42 })));
        SystemKeyspaceMigrator41.migrateTransferredRanges();

        int rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", tab)))
        {
            rowCount++;
            assertEquals("foo", row.getString("operation"));
            assertEquals(InetAddress.getByName("127.0.0.1"), row.getInetAddress("peer"));
            assertEquals(DatabaseDescriptor.getStoragePort(), row.getInt("peer_port"));
            assertEquals("bar", row.getString("keyspace_name"));
            assertEquals(ImmutableSet.of(ByteBuffer.wrap(new byte[] { 42 })), row.getSet("ranges", BytesType.instance));
        }
        assertEquals(1, rowCount);

        //Test nulls/missing don't prevent the row from propagating
        execute(String.format("TRUNCATE %s", legacyTab));
        execute(String.format("TRUNCATE %s", tab));

        execute(String.format("INSERT INTO %s (operation, peer, keyspace_name) VALUES (?, ?, ?)", legacyTab),
                "foo",
                InetAddress.getByName("127.0.0.1"),
                "bar");
        SystemKeyspaceMigrator41.migrateTransferredRanges();

        rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", tab)))
        {
            rowCount++;
        }
        assertEquals(1, rowCount);
    }

    @Test
    public void testMigrateAvailableRanges() throws Throwable
    {
        String legacyTab = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_AVAILABLE_RANGES);
        String tab = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.AVAILABLE_RANGES_V2);
        Range<Token> testRange = new Range<>(DatabaseDescriptor.getPartitioner().getRandomToken(), DatabaseDescriptor.getPartitioner().getRandomToken());
        String insert = String.format("INSERT INTO %s ("
                                      + "keyspace_name, "
                                      + "ranges) "
                                      + " values ( ?, ? )",
                                      legacyTab);
        execute(insert,
                "foo",
                ImmutableSet.of(SystemKeyspace.rangeToBytes(testRange)));
        SystemKeyspaceMigrator41.migrateAvailableRanges();

        int rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", tab)))
        {
            rowCount++;
            assertEquals("foo", row.getString("keyspace_name"));
            assertEquals(ImmutableSet.of(testRange), SystemKeyspace.rawRangesToRangeSet(row.getSet("full_ranges", BytesType.instance), DatabaseDescriptor.getPartitioner()));
        }
        assertEquals(1, rowCount);
    }

    @Test
    public void testMigrateSSTableActivity() throws Throwable
    {
        FBUtilities.setPreviousReleaseVersionString(CassandraVersion.NULL_VERSION.toString());
        String legacyTab = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_SSTABLE_ACTIVITY);
        String tab = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SSTABLE_ACTIVITY_V2);

        String insert = String.format("INSERT INTO %s (%s) VALUES (%s)",
                                      legacyTab,
                                      StringUtils.join(new String[] {"keyspace_name",
                                                       "columnfamily_name",
                                                       "generation",
                                                       "rate_120m",
                                                       "rate_15m"}, ", "),
                                      StringUtils.repeat("?", ", ", 5));

        execute(insert, "ks", "tab", 5, 123.234d, 345.456d);

        ColumnFamilyStore cf = getColumnFamilyStore(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SSTABLE_ACTIVITY_V2);
        cf.truncateBlocking();
        cf.clearUnsafe();
        SystemKeyspaceMigrator41.migrateSSTableActivity();

        int rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", tab)))
        {
            rowCount++;
            assertEquals("ks", row.getString("keyspace_name"));
            assertEquals("tab", row.getString("table_name"));
            assertEquals(new SequenceBasedSSTableId(5).toString(), row.getString("id"));
            assertEquals(123.234d, row.getDouble("rate_120m"), 0.001d);
            assertEquals(345.456d, row.getDouble("rate_15m"), 0.001d);
        }
        assertEquals(1, rowCount);
    }
    
    @Test
    public void testMigrateCompactionHistory() throws Throwable
    {
        String table = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COMPACTION_HISTORY);
        String insert = String.format("INSERT INTO %s ("
                                      + "id, "
                                      + "bytes_in, "
                                      + "bytes_out, "
                                      + "columnfamily_name, "
                                      + "compacted_at, "
                                      + "keyspace_name, "
                                      + "rows_merged) "
                                      + " values ( ?, ?, ?, ?, ?, ?, ? )",
                                      table);
        TimeUUID compactionId = TimeUUID.Generator.atUnixMillis(currentTimeMillis());
        Date compactAt  = Date.from(now());
        Map<Integer, Long> rowsMerged = ImmutableMap.of(6, 1L);
        execute(insert,
                compactionId,
                10L,
                5L,
                "table",
                compactAt,
                "keyspace",
                rowsMerged);
        SystemKeyspaceMigrator41.migrateCompactionHistory();

        int rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s where keyspace_name = 'keyspace' and columnfamily_name = 'table' allow filtering", table)))
        {
            rowCount++;
            assertEquals(compactionId, row.getTimeUUID("id"));
            assertEquals(10L, row.getLong("bytes_in"));
            assertEquals(5L, row.getLong("bytes_out"));
            assertEquals("table", row.getString("columnfamily_name"));
            assertEquals(compactAt, row.getTimestamp("compacted_at"));
            assertEquals("keyspace", row.getString("keyspace_name"));
            assertEquals(rowsMerged, row.getMap("rows_merged", Int32Type.instance, LongType.instance));
            assertEquals(ImmutableMap.of(), row.getMap("compaction_properties", UTF8Type.instance, UTF8Type.instance));
        }
        assertEquals(1, rowCount);

        //Test nulls/missing don't prevent the row from propagating
        execute(String.format("TRUNCATE %s", table));
        
        execute(String.format("INSERT INTO %s (id) VALUES (?)", table), compactionId);
        SystemKeyspaceMigrator41.migrateCompactionHistory();

        assertEquals(1, execute(String.format("SELECT * FROM %s", table)).size());
    }
}
