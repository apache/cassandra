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
package org.apache.cassandra.service.paxos;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaxosStateBlobTest extends CQLTester
{
    private static final long MICROS = 1_700_000_000_000_000L;

    private TableMetadata metadata;
    private Config.PaxosStatePurging savedPurging;
    private ListAppender<ILoggingEvent> appender;
    private ch.qos.logback.classic.Logger rootLogger;

    @Before
    public void setUpTable()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        metadata = currentTableMetadata();
        savedPurging = DatabaseDescriptor.paxosStatePurging();
        DatabaseDescriptor.setPaxosStatePurging(Config.PaxosStatePurging.legacy);

        appender = new ListAppender<>();
        appender.start();
        rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(appender);
    }

    @After
    public void restore()
    {
        rootLogger.detachAppender(appender);
        DatabaseDescriptor.setPaxosStatePurging(savedPurging);
    }

    private DecoratedKey key(int k)
    {
        return metadata.partitioner.decorateKey(ByteBufferUtil.bytes(k));
    }

    private PartitionUpdate update(int k, int value, long timestamp)
    {
        return UpdateBuilder.create(metadata, k).withTimestamp(timestamp).newRow().add("v", value).build();
    }

    private int valueOf(PartitionUpdate update)
    {
        return ByteBufferUtil.toInt(update.getRow(Clustering.EMPTY).getCell(metadata.getColumn(ByteBufferUtil.bytes("v"))).buffer());
    }

    private boolean loggedAtWarnOrAbove()
    {
        for (ILoggingEvent event : new ArrayList<>(appender.list))
            if (event.getLevel().isGreaterOrEqual(Level.WARN))
                return true;
        return false;
    }

    private void tiedCommitsLoadAsOneRound(int k, boolean higherBallotFirst)
    {
        Ballot high = Ballot.atUnixMicrosWithLsb(MICROS, 2L, Ballot.Flag.LOCAL);
        Ballot low = Ballot.atUnixMicrosWithLsb(MICROS, 1L, Ballot.Flag.LOCAL);
        // The higher ballot carries the smaller value, so the ballot cell and the blob cell would pick
        // different rounds if they were read separately.
        Commit highCommit = new Commit(high, update(k, 1, MICROS));
        Commit lowCommit = new Commit(low, update(k, 2, MICROS));

        SystemKeyspace.savePaxosCommit(higherBallotFirst ? highCommit : lowCommit);
        SystemKeyspace.savePaxosCommit(higherBallotFirst ? lowCommit : highCommit);

        PaxosState.Snapshot snapshot = SystemKeyspace.loadPaxosState(key(k), metadata, 0);
        Ballot loaded = snapshot.committed.ballot;
        Assert.assertTrue(loaded.equals(high) || loaded.equals(low));
        Assert.assertEquals(loaded.equals(high) ? 1 : 2, valueOf(snapshot.committed.update));
        Assert.assertFalse(loggedAtWarnOrAbove());
    }

    @Test
    public void tiedCommitsLoadAsOneRoundHighBallotFirst()
    {
        tiedCommitsLoadAsOneRound(1, true);
    }

    @Test
    public void tiedCommitsLoadAsOneRoundLowBallotFirst()
    {
        tiedCommitsLoadAsOneRound(2, false);
    }

    @Test
    public void proposalAndCommitLoadFromTheirBlobs()
    {
        Ballot proposalBallot = Ballot.atUnixMicrosWithLsb(MICROS + 10, 5L, Ballot.Flag.LOCAL);
        Ballot commitBallot = Ballot.atUnixMicrosWithLsb(MICROS, 6L, Ballot.Flag.LOCAL);

        SystemKeyspace.savePaxosCommit(new Commit(commitBallot, update(3, 11, MICROS)));
        SystemKeyspace.savePaxosProposal(new Commit(proposalBallot, update(3, 12, MICROS + 10)));

        PaxosState.Snapshot snapshot = SystemKeyspace.loadPaxosState(key(3), metadata, 0);
        Assert.assertEquals(commitBallot, snapshot.committed.ballot);
        Assert.assertEquals(11, valueOf(snapshot.committed.update));
        Assert.assertNotNull(snapshot.accepted);
        Assert.assertEquals(proposalBallot, snapshot.accepted.ballot);
        Assert.assertEquals(12, valueOf(snapshot.accepted.update));
    }

    private void writeLegacyCommit(int k, Ballot ballot, int value, int blobVersion, int columnVersion)
    {
        String cql = String.format("UPDATE %s.%s USING TIMESTAMP ? SET most_recent_commit_at = ?, most_recent_commit = ?, most_recent_commit_version = ? WHERE row_key = ? AND cf_id = ?",
                                   SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PAXOS);
        ByteBuffer blob = PartitionUpdate.toBytes(update(k, value, ballot.unixMicros()), blobVersion);
        QueryProcessor.executeInternal(cql, ballot.unixMicros(), ballot, blob, columnVersion, key(k).getKey(), metadata.id.asUUID());
    }

    @Test
    public void legacyBlobUnderANewerVersionColumnStillLoads()
    {
        Ballot ballot = Ballot.atUnixMicrosWithLsb(MICROS, 7L, Ballot.Flag.LOCAL);
        writeLegacyCommit(4, ballot, 21, MessagingService.VERSION_40, MessagingService.current_version);

        PaxosState.Snapshot snapshot = SystemKeyspace.loadPaxosState(key(4), metadata, 0);
        Assert.assertEquals(ballot, snapshot.committed.ballot);
        Assert.assertEquals(21, valueOf(snapshot.committed.update));
        Assert.assertFalse(loggedAtWarnOrAbove());
    }

    @Test
    public void legacyBlobIsNeverLoadedAboveItsColumnVersion()
    {
        Ballot ballot = Ballot.atUnixMicrosWithLsb(MICROS, 8L, Ballot.Flag.LOCAL);
        writeLegacyCommit(5, ballot, 22, MessagingService.current_version, MessagingService.VERSION_40);

        assertThatThrownBy(() -> SystemKeyspace.loadPaxosState(key(5), metadata, 0)).isInstanceOf(MarshalException.class);
    }

    @Test
    public void anEmptyProposalLoadsAsAnEmptyAcceptedUpdate()
    {
        Ballot ballot = Ballot.atUnixMicrosWithLsb(MICROS, 10L, Ballot.Flag.LOCAL);
        SystemKeyspace.savePaxosProposal(new Commit(ballot, PartitionUpdate.emptyUpdate(metadata, key(7))));

        PaxosState.Snapshot snapshot = SystemKeyspace.loadPaxosState(key(7), metadata, 0);
        Assert.assertNotNull(snapshot.accepted);
        Assert.assertEquals(ballot, snapshot.accepted.ballot);
        Assert.assertTrue(snapshot.accepted.update.isEmpty());
        Assert.assertFalse(loggedAtWarnOrAbove());
    }

    @Test
    public void commitWritesTheTrueMessagingVersionToTheVersionColumn()
    {
        Ballot ballot = Ballot.atUnixMicrosWithLsb(MICROS, 9L, Ballot.Flag.LOCAL);
        SystemKeyspace.savePaxosCommit(new Commit(ballot, update(6, 31, MICROS)));

        String cql = String.format("SELECT most_recent_commit_version FROM %s.%s WHERE row_key = ? AND cf_id = ?",
                                   SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PAXOS);
        Assert.assertEquals(MessagingService.current_version,
                            QueryProcessor.executeInternal(cql, key(6).getKey(), metadata.id.asUUID()).one().getInt("most_recent_commit_version"));
    }
}
