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
import java.util.UUID;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;

public class SystemKeyspaceMigrator40Test extends CQLTester
{
    @Test
    public void testMigratePeers() throws Throwable
    {
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
                                      SystemKeyspaceMigrator40.legacyPeersName);
        UUID hostId = UUIDGen.getTimeUUID();
        UUID schemaVersion = UUIDGen.getTimeUUID();
        execute(insert,
                InetAddress.getByName("127.0.0.1"),
                "dcFoo",
                hostId,
                InetAddress.getByName("127.0.0.2"),
                "rackFoo", "4.0",
                InetAddress.getByName("127.0.0.3"),
                schemaVersion,
                ImmutableSet.of("foobar"));
        SystemKeyspaceMigrator40.migrate();

        int rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", SystemKeyspaceMigrator40.peersName)))
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
        execute(String.format("TRUNCATE %s", SystemKeyspaceMigrator40.legacyPeersName));
        execute(String.format("TRUNCATE %s", SystemKeyspaceMigrator40.peersName));

        execute(String.format("INSERT INTO %s (peer) VALUES (?)", SystemKeyspaceMigrator40.legacyPeersName),
                              InetAddress.getByName("127.0.0.1"));
        SystemKeyspaceMigrator40.migrate();

        rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", SystemKeyspaceMigrator40.peersName)))
        {
            rowCount++;
        }
        assertEquals(1, rowCount);
    }

    @Test
    public void testMigratePeerEvents() throws Throwable
    {
        String insert = String.format("INSERT INTO %s ("
                                      + "peer, "
                                      + "hints_dropped) "
                                      + " values ( ?, ? )",
                                      SystemKeyspaceMigrator40.legacyPeerEventsName);
        UUID uuid = UUIDGen.getTimeUUID();
        execute(insert,
                InetAddress.getByName("127.0.0.1"),
                ImmutableMap.of(uuid, 42));
        SystemKeyspaceMigrator40.migrate();

        int rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", SystemKeyspaceMigrator40.peerEventsName)))
        {
            rowCount++;
            assertEquals(InetAddress.getByName("127.0.0.1"), row.getInetAddress("peer"));
            assertEquals(DatabaseDescriptor.getStoragePort(), row.getInt("peer_port"));
            assertEquals(ImmutableMap.of(uuid, 42), row.getMap("hints_dropped", UUIDType.instance, Int32Type.instance));
        }
        assertEquals(1, rowCount);

        //Test nulls/missing don't prevent the row from propagating
        execute(String.format("TRUNCATE %s", SystemKeyspaceMigrator40.legacyPeerEventsName));
        execute(String.format("TRUNCATE %s", SystemKeyspaceMigrator40.peerEventsName));

        execute(String.format("INSERT INTO %s (peer) VALUES (?)", SystemKeyspaceMigrator40.legacyPeerEventsName),
                InetAddress.getByName("127.0.0.1"));
        SystemKeyspaceMigrator40.migrate();

        rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", SystemKeyspaceMigrator40.peerEventsName)))
        {
            rowCount++;
        }
        assertEquals(1, rowCount);
    }

    @Test
    public void testMigrateTransferredRanges() throws Throwable
    {
        String insert = String.format("INSERT INTO %s ("
                                      + "operation, "
                                      + "peer, "
                                      + "keyspace_name, "
                                      + "ranges) "
                                      + " values ( ?, ?, ?, ? )",
                                      SystemKeyspaceMigrator40.legacyTransferredRangesName);
        execute(insert,
                "foo",
                InetAddress.getByName("127.0.0.1"),
                "bar",
                ImmutableSet.of(ByteBuffer.wrap(new byte[] { 42 })));
        SystemKeyspaceMigrator40.migrate();

        int rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", SystemKeyspaceMigrator40.transferredRangesName)))
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
        execute(String.format("TRUNCATE %s", SystemKeyspaceMigrator40.legacyTransferredRangesName));
        execute(String.format("TRUNCATE %s", SystemKeyspaceMigrator40.transferredRangesName));

        execute(String.format("INSERT INTO %s (operation, peer, keyspace_name) VALUES (?, ?, ?)", SystemKeyspaceMigrator40.legacyTransferredRangesName),
                "foo",
                InetAddress.getByName("127.0.0.1"),
                "bar");
        SystemKeyspaceMigrator40.migrate();

        rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", SystemKeyspaceMigrator40.transferredRangesName)))
        {
            rowCount++;
        }
        assertEquals(1, rowCount);
    }

    @Test
    public void testMigrateAvailableRanges() throws Throwable
    {
        Range<Token> testRange = new Range<>(DatabaseDescriptor.getPartitioner().getRandomToken(), DatabaseDescriptor.getPartitioner().getRandomToken());
        String insert = String.format("INSERT INTO %s ("
                                      + "keyspace_name, "
                                      + "ranges) "
                                      + " values ( ?, ? )",
                                      SystemKeyspaceMigrator40.legacyAvailableRangesName);
        execute(insert,
                "foo",
                ImmutableSet.of(SystemKeyspace.rangeToBytes(testRange)));
        SystemKeyspaceMigrator40.migrate();

        int rowCount = 0;
        for (UntypedResultSet.Row row : execute(String.format("SELECT * FROM %s", SystemKeyspaceMigrator40.availableRangesName)))
        {
            rowCount++;
            assertEquals("foo", row.getString("keyspace_name"));
            assertEquals(ImmutableSet.of(testRange), SystemKeyspace.rawRangesToRangeSet(row.getSet("full_ranges", BytesType.instance), DatabaseDescriptor.getPartitioner()));
        }
        assertEquals(1, rowCount);
    }
}
