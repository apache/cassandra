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

package org.apache.cassandra.service.reads.repair;

import java.net.UnknownHostException;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class RepairedDataVerifierTest
{
    private static final String TEST_NAME = "read_command_vh_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace";
    private static final String TABLE = "table1";

    private final Random random = new Random();
    private TableMetadata metadata;
    private TableMetrics metrics;

    // counter to generate the last byte of peer addresses
    private int addressSuffix = 10;

    @BeforeClass
    public static void init()
    {
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition(TEST_NAME);
        DatabaseDescriptor.reportUnconfirmedRepairedDataMismatches(true);
    }

    @Before
    public void setup()
    {
        metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        metrics = ColumnFamilyStore.metricsFor(metadata.id);
    }

    @Test
    public void repairedDataMismatchWithSomeConclusive()
    {
        long confirmedCount =  confirmedCount();
        long unconfirmedCount =  unconfirmedCount();
        InetAddressAndPort peer1 = peer();
        InetAddressAndPort peer2 = peer();
        RepairedDataVerifier.SimpleVerifier verifier = new RepairedDataVerifier.SimpleVerifier(command(key()));
        RepairedDataTracker tracker = new RepairedDataTracker(verifier);
        tracker.recordDigest(peer1, ByteBufferUtil.bytes("digest1"), false);
        tracker.recordDigest(peer2, ByteBufferUtil.bytes("digest2"), true);

        tracker.verify();
        assertEquals(confirmedCount, confirmedCount());
        assertEquals(unconfirmedCount + 1 , unconfirmedCount());
    }

    @Test
    public void repairedDataMismatchWithNoneConclusive()
    {
        long confirmedCount =  confirmedCount();
        long unconfirmedCount =  unconfirmedCount();
        InetAddressAndPort peer1 = peer();
        InetAddressAndPort peer2 = peer();
        RepairedDataVerifier.SimpleVerifier verifier = new RepairedDataVerifier.SimpleVerifier(command(key()));
        RepairedDataTracker tracker = new RepairedDataTracker(verifier);
        tracker.recordDigest(peer1, ByteBufferUtil.bytes("digest1"), false);
        tracker.recordDigest(peer2, ByteBufferUtil.bytes("digest2"), false);

        tracker.verify();
        assertEquals(confirmedCount, confirmedCount());
        assertEquals(unconfirmedCount + 1 , unconfirmedCount());
    }

    @Test
    public void repairedDataMismatchWithAllConclusive()
    {
        long confirmedCount =  confirmedCount();
        long unconfirmedCount =  unconfirmedCount();
        InetAddressAndPort peer1 = peer();
        InetAddressAndPort peer2 = peer();
        RepairedDataVerifier.SimpleVerifier verifier = new RepairedDataVerifier.SimpleVerifier(command(key()));
        RepairedDataTracker tracker = new RepairedDataTracker(verifier);
        tracker.recordDigest(peer1, ByteBufferUtil.bytes("digest1"), true);
        tracker.recordDigest(peer2, ByteBufferUtil.bytes("digest2"), true);

        tracker.verify();
        assertEquals(confirmedCount + 1, confirmedCount());
        assertEquals(unconfirmedCount, unconfirmedCount());
    }

    @Test
    public void repairedDataMatchesWithAllConclusive()
    {
        long confirmedCount =  confirmedCount();
        long unconfirmedCount =  unconfirmedCount();
        InetAddressAndPort peer1 = peer();
        InetAddressAndPort peer2 = peer();
        RepairedDataVerifier.SimpleVerifier verifier = new RepairedDataVerifier.SimpleVerifier(command(key()));
        RepairedDataTracker tracker = new RepairedDataTracker(verifier);
        tracker.recordDigest(peer1, ByteBufferUtil.bytes("digest1"), true);
        tracker.recordDigest(peer2, ByteBufferUtil.bytes("digest1"), true);

        tracker.verify();
        assertEquals(confirmedCount, confirmedCount());
        assertEquals(unconfirmedCount, unconfirmedCount());
    }

    @Test
    public void repairedDataMatchesWithSomeConclusive()
    {
        long confirmedCount =  confirmedCount();
        long unconfirmedCount =  unconfirmedCount();
        InetAddressAndPort peer1 = peer();
        InetAddressAndPort peer2 = peer();
        RepairedDataVerifier.SimpleVerifier verifier = new RepairedDataVerifier.SimpleVerifier(command(key()));
        RepairedDataTracker tracker = new RepairedDataTracker(verifier);
        tracker.recordDigest(peer1, ByteBufferUtil.bytes("digest1"), true);
        tracker.recordDigest(peer2, ByteBufferUtil.bytes("digest1"), false);

        tracker.verify();
        assertEquals(confirmedCount, confirmedCount());
        assertEquals(unconfirmedCount, unconfirmedCount());
    }

    @Test
    public void repairedDataMatchesWithNoneConclusive()
    {
        long confirmedCount =  confirmedCount();
        long unconfirmedCount =  unconfirmedCount();
        InetAddressAndPort peer1 = peer();
        InetAddressAndPort peer2 = peer();
        RepairedDataVerifier.SimpleVerifier verifier = new RepairedDataVerifier.SimpleVerifier(command(key()));
        RepairedDataTracker tracker = new RepairedDataTracker(verifier);
        tracker.recordDigest(peer1, ByteBufferUtil.bytes("digest1"), false);
        tracker.recordDigest(peer2, ByteBufferUtil.bytes("digest1"), false);

        tracker.verify();
        assertEquals(confirmedCount, confirmedCount());
        assertEquals(unconfirmedCount, unconfirmedCount());
    }

    @Test
    public void allEmptyDigestWithAllConclusive()
    {
        // if a read didn't touch any repaired sstables, digests will be empty
        long confirmedCount =  confirmedCount();
        long unconfirmedCount =  unconfirmedCount();
        InetAddressAndPort peer1 = peer();
        InetAddressAndPort peer2 = peer();
        RepairedDataVerifier.SimpleVerifier verifier = new RepairedDataVerifier.SimpleVerifier(command(key()));
        RepairedDataTracker tracker = new RepairedDataTracker(verifier);
        tracker.recordDigest(peer1, ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        tracker.recordDigest(peer2, ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        tracker.verify();
        assertEquals(confirmedCount, confirmedCount());
        assertEquals(unconfirmedCount, unconfirmedCount());
    }

    @Test
    public void allEmptyDigestsWithSomeConclusive()
    {
        // if a read didn't touch any repaired sstables, digests will be empty
        long confirmedCount =  confirmedCount();
        long unconfirmedCount =  unconfirmedCount();
        InetAddressAndPort peer1 = peer();
        InetAddressAndPort peer2 = peer();
        RepairedDataVerifier.SimpleVerifier verifier = new RepairedDataVerifier.SimpleVerifier(command(key()));
        RepairedDataTracker tracker = new RepairedDataTracker(verifier);
        tracker.recordDigest(peer1, ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        tracker.recordDigest(peer2, ByteBufferUtil.EMPTY_BYTE_BUFFER, false);

        tracker.verify();
        assertEquals(confirmedCount, confirmedCount());
        assertEquals(unconfirmedCount, unconfirmedCount());
    }

    @Test
    public void allEmptyDigestsWithNoneConclusive()
    {
        // if a read didn't touch any repaired sstables, digests will be empty
        long confirmedCount =  confirmedCount();
        long unconfirmedCount =  unconfirmedCount();
        InetAddressAndPort peer1 = peer();
        InetAddressAndPort peer2 = peer();
        RepairedDataVerifier.SimpleVerifier verifier = new RepairedDataVerifier.SimpleVerifier(command(key()));
        RepairedDataTracker tracker = new RepairedDataTracker(verifier);
        tracker.recordDigest(peer1, ByteBufferUtil.EMPTY_BYTE_BUFFER, false);
        tracker.recordDigest(peer2, ByteBufferUtil.EMPTY_BYTE_BUFFER, false);

        tracker.verify();
        assertEquals(confirmedCount, confirmedCount());
        assertEquals(unconfirmedCount, unconfirmedCount());
    }

    @Test
    public void noTrackingDataRecorded()
    {
        // if a read didn't land on any replicas which support repaired data tracking, nothing will be recorded
        long confirmedCount =  confirmedCount();
        long unconfirmedCount =  unconfirmedCount();
        RepairedDataVerifier.SimpleVerifier verifier = new RepairedDataVerifier.SimpleVerifier(command(key()));
        RepairedDataTracker tracker = new RepairedDataTracker(verifier);
        tracker.verify();
        assertEquals(confirmedCount, confirmedCount());
        assertEquals(unconfirmedCount, unconfirmedCount());
    }

    private long confirmedCount()
    {
        return metrics.confirmedRepairedInconsistencies.table.getCount();
    }

    private long unconfirmedCount()
    {
        return metrics.unconfirmedRepairedInconsistencies.table.getCount();
    }

    private InetAddressAndPort peer()
    {
        try
        {
            return InetAddressAndPort.getByAddress(new byte[]{ 127, 0, 0, (byte) addressSuffix++ });
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private int key()
    {
        return random.nextInt();
    }

    private ReadCommand command(int key)
    {
        return new StubReadCommand(key, metadata, false);
    }

    private static class StubReadCommand extends SinglePartitionReadCommand
    {
        StubReadCommand(int key, TableMetadata metadata, boolean isDigest)
        {
            super(isDigest,
                  0,
                  false,
                  metadata,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(metadata),
                  RowFilter.none(),
                  DataLimits.NONE,
                  metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key)),
                  new ClusteringIndexSliceFilter(Slices.ALL, false),
                  null,
                  false);
        }
    }
}
