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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config.PaxosVariant;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SimpleBuilders.PartitionUpdateBuilder;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.consensus.migration.ConsensusKeyMigrationState;
import org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter;
import org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState;
import org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.ConsensusMigrationState;
import org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.ConsensusMigrationTarget;
import org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.TableMigrationState;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Ballot.Flag;
import org.apache.cassandra.service.paxos.BallotGenerator;
import org.apache.cassandra.service.paxos.Commit.Agreed;
import org.apache.cassandra.service.paxos.Commit.Proposal;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.PojoToString;
import org.yaml.snakeyaml.Yaml;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.apache.cassandra.Util.spinUntilSuccess;
import static org.apache.cassandra.db.SystemKeyspace.CONSENSUS_MIGRATION_STATE;
import static org.apache.cassandra.db.SystemKeyspace.PAXOS;
import static org.apache.cassandra.dht.Range.normalize;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ANY;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.ConsensusRoutingDecision.paxosV2;
import static org.apache.cassandra.service.paxos.PaxosState.MaybePromise.Outcome.PROMISE;
import static org.assertj.core.api.Fail.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/*
 * This test suite is intended to serve as an integration test with some pretty good visibility into actual execution
 * that can run quickly, and make sure all the right steps are running during migration.
 *
 * For correctness related to wrong/right answers we rely on simulator to validate.
 */
public class AccordMigrationTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordMigrationTest.class);

    private static final int CLUSTERING_VALUE = 2;

    private static final String TABLE_FMT = "CREATE TABLE %s (id int, c int, v int, s int static, PRIMARY KEY ((id), c));";

    private static final String CAS_FMT = "UPDATE %s SET v = 4 WHERE id = ? AND c = %d IF v = 42";

    private static IPartitioner partitioner;

    private static Token minToken;

    private static Token maxToken;

    private static Token midToken;

    private static Token upperMidToken;

    private static Token lowerMidToken;

    private static ICoordinator coordinator;

    // To create a precise repair where the repaired range is fully contained in a locally replicated range
    // we need to align with this token. The local ranges are (9223372036854775805,-1] and (-1,9223372036854775805]
    // No idea why the partitioner creates such an
    private Token maxAlignedWithLocalRanges = new LongToken(9223372036854775805L);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        ServerTestUtils.daemonInitialization();
        // Otherwise repair complains if you don't specify a keyspace
        CassandraRelevantProperties.SYSTEM_TRACES_DEFAULT_RF.setInt(3);
        AccordTestBase.setupCluster(builder ->
                                        builder.appendConfig(config ->
                                                             config.set("paxos_variant", PaxosVariant.v2.name())
                                                                   .set("non_serial_write_strategy", "migration")),
                                    3);
        partitioner = FBUtilities.newPartitioner(SHARED_CLUSTER.get(1).callsOnInstance(() -> DatabaseDescriptor.getPartitioner().getClass().getSimpleName()).call());
        StorageService.instance.setPartitionerUnsafe(partitioner);
        ServerTestUtils.prepareServerNoRegister();
        minToken = partitioner.getMinimumToken();
        maxToken = partitioner.getMaximumToken();
        midToken = partitioner.midpoint(minToken, maxToken);
        upperMidToken = partitioner.midpoint(midToken, maxToken);
        lowerMidToken = partitioner.midpoint(minToken, midToken);
        coordinator = SHARED_CLUSTER.coordinator(1);
    }

    @AfterClass
    public static void tearDownClass()
    {
        StorageService.instance.resetPartitionerUnsafe();
    }

    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
        // Reset migration state
        forEach(() -> {
            ConsensusRequestRouter.resetInstance();
            ConsensusKeyMigrationState.reset();
        });
        SHARED_CLUSTER.get(1).runOnInstance(() -> {
            ConsensusTableMigrationState.reset();
        });
        SHARED_CLUSTER.coordinators().forEach(coordinator -> coordinator.execute(format("TRUNCATE TABLE %s.%s", SYSTEM_KEYSPACE_NAME, CONSENSUS_MIGRATION_STATE), ALL));
        SHARED_CLUSTER.coordinators().forEach(coordinator -> coordinator.execute(format("TRUNCATE TABLE %s.%s", SYSTEM_KEYSPACE_NAME, PAXOS), ALL));
    }

    private static String nodetool(ICoordinator coordinator, String... commandAndArgs)
    {
        NodeToolResult nodetoolResult = coordinator.instance().nodetoolResult(commandAndArgs);
        if (!nodetoolResult.getStdout().isEmpty())
            System.out.println(nodetoolResult.getStdout());
        if (!nodetoolResult.getStderr().isEmpty())
            System.err.println(nodetoolResult.getStderr());
        if (nodetoolResult.getError() != null)
            fail("Failed nodetool " + Arrays.asList(commandAndArgs), nodetoolResult.getError());
        // TODO why does standard out end up in stderr in nodetool?
        return nodetoolResult.getStdout();
    }

    private static int getKeyBetweenTokens(Token left, Token right)
    {
        return getKeysBetweenTokens(left, right).next();
    }

    private static Iterator<Integer> getKeysBetweenTokens(Token left, Token right)
    {
        return new Iterator<Integer>()
        {
            int candidate = 0;
            @Override
            public boolean hasNext()
            {
                return true;
            }

            @Override
            public Integer next()
            {
                for (int i = 0; i < 1_000_000; i++)
                {
                    int value = candidate;
                    candidate++;
                    if (partitioner.getToken(ByteBufferUtil.bytes(value)).compareTo(right) < 0 && partitioner.getToken(ByteBufferUtil.bytes(value)).compareTo(left) > 0)
                        return value;
                }
                throw new IllegalStateException("Gave up after 1 million attempts");
            }
        };
    }

    /*
     * Force routing a request to Paxos even after a range has been marked migrating to simulate
     * a race between updating cluster metadata and making a routing decision to a specific consensus
     * protocol. Paxos should still detect the routing change at two points. After running the promise phase
     * (round of messaging might discover a new epoch) and during the accept phase (might not get a majority due
     * to rejects caused by acceptors refusing due to migration).
     *
     * This is used directly to test that begin rejects after discovering a migration, and indirectly in
     * PaxosToAccordMigrationNotHappeningUpToAccept.
     */
    public static class RoutesToPaxosOnce extends ConsensusRequestRouter
    {
        boolean routed;

        @Override
        protected ConsensusRoutingDecision routeAndMaybeMigrate(@Nonnull DecoratedKey key, @Nonnull ColumnFamilyStore cfs, ConsistencyLevel consistencyLevel, long queryStartNanoTime, long timeoutNanos, boolean isForWrite)
        {
            if (routed)
                return super.routeAndMaybeMigrate(key, cfs, consistencyLevel, queryStartNanoTime, timeoutNanos, isForWrite);
            routed = true;
            return paxosV2;
        }
    }

    /*
     * To allow for testing of Paxos we want to force begin to succeed, but accept to fail
     * with a retry on new protocol reject.
     */
    public static class PaxosToAccordMigrationNotHappeningUpToBegin extends RoutesToPaxosOnce
    {
        @Override
        public boolean isKeyInMigratingOrMigratedRangeDuringPaxosBegin(TableId tableId, DecoratedKey key)
        {
            return false;
        }
    }

    public static class PaxosToAccordMigrationNotHappeningUpToAccept extends PaxosToAccordMigrationNotHappeningUpToBegin
    {
        @Override
        public boolean isKeyInMigratingOrMigratedRangeDuringPaxosAccept(TableId tableId, DecoratedKey key)
        {
            return false;
        }
    }

    public static class RoutesToAccordOnce extends ConsensusRequestRouter
    {
        boolean routed;

        @Override
        protected ConsensusRoutingDecision routeAndMaybeMigrate(@Nonnull DecoratedKey key, @Nonnull ColumnFamilyStore cfs, ConsistencyLevel consistencyLevel, long queryStartNanoTime, long timeoutNanos, boolean isForWrite)
        {
            if (routed)
                return super.routeAndMaybeMigrate(key, cfs, consistencyLevel, queryStartNanoTime, timeoutNanos, isForWrite);
            routed = true;
            return ConsensusRoutingDecision.accord;
        }
    }

    /*
     * Helper to invoke a query and assert that the right metrics change indicating the correct
     * paths were taken to execute the query during migration
     */
    private static void assertTargetAccordWrite(Consumer<Integer> query, int coordinatorIndex, int key, int expectedAccordWriteCount, int expectedCasWriteCount, int expectedKeyMigrationCount, int expectedCasBeginRejects, int expectedCasAcceptRejects)
    {
        int startingWriteCount = getAccordWriteCount(coordinatorIndex);
        int startingCasWriteCount = getCasWriteCount(coordinatorIndex);
        int startingKeyMigrationCount = getKeyMigrationCount(coordinatorIndex);
        int startingCasWriteBeginRejects = getCasWriteBeginRejects(coordinatorIndex);
        int startingCasWriteAcceptRejects = getCasWriteAcceptRejects(coordinatorIndex);
        query.accept(key);
        assertEquals("Accord writes", expectedAccordWriteCount, getAccordWriteCount(coordinatorIndex) - startingWriteCount);
        assertEquals("CAS writes", expectedCasWriteCount, getCasWriteCount(coordinatorIndex) - startingCasWriteCount);
        assertEquals("Key Migrations", expectedKeyMigrationCount, getKeyMigrationCount(coordinatorIndex) - startingKeyMigrationCount);
        assertEquals("CAS Begin rejects", expectedCasBeginRejects, getCasWriteBeginRejects(coordinatorIndex) - startingCasWriteBeginRejects);
        assertEquals("CAS Accept rejects", expectedCasAcceptRejects, getCasWriteAcceptRejects(coordinatorIndex) - startingCasWriteAcceptRejects);
    }

    private static Object[][] assertTargetAccordRead(Function<Integer, Object[][]> query, int coordinatorIndex, int key, int expectedAccordReadCount, int expectedCasPrepareCount, int expectedKeyMigrationCount, int expectedCasReadBeginRejects, int expectedCasReadAcceptRejects)
    {
        int startingReadCount = getAccordReadCount(coordinatorIndex);
        int startingCasPrepareCount = getCasPrepareCount(coordinatorIndex);
        int startingKeyMigrationCount = getKeyMigrationCount(coordinatorIndex);
        int startingCasReadBeginRejects = getCasReadBeginRejects(coordinatorIndex);
        int startingCasReadAcceptRejects = getCasReadAcceptRejects(coordinatorIndex);
        Object[][] result = query.apply(key);
        assertEquals("Accord reads", expectedAccordReadCount, getAccordReadCount(coordinatorIndex) - startingReadCount);
        assertEquals("CAS prepares", expectedCasPrepareCount, getCasPrepareCount(coordinatorIndex) - startingCasPrepareCount);
        assertEquals("Key Migrations", expectedKeyMigrationCount, getKeyMigrationCount(coordinatorIndex) - startingKeyMigrationCount);
        assertEquals("CAS Begin rejects", expectedCasReadBeginRejects, getCasReadBeginRejects(coordinatorIndex) - startingCasReadBeginRejects);
        assertEquals("CAS Accept rejects", expectedCasReadAcceptRejects, getCasReadAcceptRejects(coordinatorIndex) - startingCasReadAcceptRejects);
        return result;
    }

    private static void assertTargetPaxosWrite(Consumer<Integer> query, int coordinatorIndex, int key, int expectedAccordWriteCount, int expectedCasWriteCount, int expectedKeyMigrationCount, int expectedMigrationRejects, int expectedSkippedReads)
    {
        int startingWriteCount = getAccordWriteCount(coordinatorIndex);
        int startingCasWriteCount = getCasWriteCount(coordinatorIndex);
        int startingKeyMigrationCount = getKeyMigrationCount(coordinatorIndex);
        int startingMigrationRejectsCount = getAccordMigrationRejects(coordinatorIndex);
        int startingSkippedReadsCount = getAccordMigrationSkippedReads();
        query.accept(key);
        assertEquals("Accord writes", expectedAccordWriteCount, getAccordWriteCount(coordinatorIndex) - startingWriteCount);
        assertEquals("CAS writes", expectedCasWriteCount, getCasWriteCount(coordinatorIndex) - startingCasWriteCount);
        assertEquals("Key Migrations", expectedKeyMigrationCount, getKeyMigrationCount(coordinatorIndex) - startingKeyMigrationCount);
        assertEquals("Accord migration rejects", expectedMigrationRejects, getAccordMigrationRejects(coordinatorIndex) - startingMigrationRejectsCount);
        assertEquals("Accord skipped reads", expectedSkippedReads, getAccordMigrationSkippedReads() - startingSkippedReadsCount);
    }

    @Test
    public void testPaxosToAccordCAS() throws Exception
    {
        test(format(TABLE_FMT, qualifiedTableName),
          cluster -> {
              String casCQL = format(CAS_FMT, qualifiedTableName, CLUSTERING_VALUE);
              Consumer<Integer> runCasNoApply = key -> assertRowEquals(cluster, new Object[]{false}, casCQL, key);
              Consumer<Integer> runCasApplies = key -> assertRowEquals(cluster, new Object[]{true}, casCQL, key);
              Consumer<Integer> runCasOnSecondNode = key -> assertEquals( "[applied]", cluster.coordinator(2).executeWithResult(casCQL, ANY, key).names().get(0));
              String tableName = qualifiedTableName.split("\\.")[1];
              int migratingKey = getKeyBetweenTokens(midToken, maxToken);
              int notMigratingKey = getKeyBetweenTokens(minToken, midToken);
              Range<Token> migratingRange = new Range(midToken, maxToken);
              List<Range<Token>> migratingRanges = ImmutableList.of(migratingRange);

              // Not actually migrating yet so should do nothing special
              assertTargetAccordWrite(runCasNoApply, 1, migratingKey, 0, 1, 0, 0, 0);

              // Mark ranges migrating and check migration state is correct
              nodetool(coordinator, "consensus_admin", "begin-migration", "-st", midToken.toString(), "-et", maxToken.toString(), "-tp", "accord", KEYSPACE, tableName);
              assertMigrationState(tableName, ConsensusMigrationTarget.accord, emptyList(), migratingRanges, 1);

              // Should be routed directly to Accord, and perform key migration, as well as key migration read in Accord
              assertTargetAccordWrite(runCasNoApply, 1, migratingKey, 1, 0, 1, 0, 0);

              // Should not repeat key migration, and should still do a migration read in Accord
              assertTargetAccordWrite(runCasNoApply, 1, migratingKey, 1, 0, 0, 0, 0);

              // Should run on Paxos since it is not in the migrating range
              assertTargetAccordWrite(runCasNoApply, 1, notMigratingKey, 0, 1, 0, 0, 0);

              // Check that the coordinator on the other node also has saved that the key migration was performed
              // and runs the query on Accord immediately without key migration
              assertTargetAccordWrite(runCasOnSecondNode, 2, migratingKey, 1, 0, 0, 0, 0);

              // Forced repair while a node is down shouldn't work, use repair instead of finish-migration because repair exposes --force
              // and regular Cassandra repairs are eligible to drive migration so it's important they check --force and down nodes
              InetAddressAndPort secondNodeBroadcastAddress = InetAddressAndPort.getByAddress(cluster.get(2).broadcastAddress());
              cluster.get(1).runOnInstance(() -> {
                  EndpointState endpointState = Gossiper.instance.getEndpointStateForEndpoint(secondNodeBroadcastAddress);
                  Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.markDead(secondNodeBroadcastAddress, endpointState));
              });
              nodetool(coordinator, "repair", "--force");
              assertMigrationState(tableName, ConsensusMigrationTarget.accord, emptyList(), migratingRanges, 1);
              cluster.get(1).runOnInstance(() -> {
                  EndpointState endpointState = Gossiper.instance.getEndpointStateForEndpoint(secondNodeBroadcastAddress);
                  Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.realMarkAlive(secondNodeBroadcastAddress, endpointState));
              });

              // Full repair should complete the migration and update the metadata, adding --force when nodes are up should be fine
              nodetool(coordinator, "repair", "--force");
              assertMigrationState(tableName, ConsensusMigrationTarget.accord, migratingRanges, emptyList(), 0);

              // Should run on Accord, and not perform key migration nor should it need to perform a migration read in Accord now that it is repaired
              assertTargetAccordWrite(runCasNoApply, 1, migratingKey, 1, 0, 0, 0, 0);

              // Should run on Paxos, and not perform key migration
              assertTargetAccordWrite(runCasNoApply, 1, notMigratingKey, 0, 1, 0, 0, 0);

              // Pivot to testing repair with a subrange of the migrating range as well as key migration
              // Will use the unmigrated range between lowerMidToken and midToken
              nodetool(coordinator, "consensus_admin", "begin-migration", "-st", lowerMidToken.toString(), "-et", midToken.toString(), "-tp", "accord", KEYSPACE, tableName);

              // Generate several keys to test with instead of resetting key state
              Iterator<Integer> testingKeys = getKeysBetweenTokens(lowerMidToken, midToken);
              migratingKey = testingKeys.next();

              // Check that Paxos repair is run and actually repairs a transaction that was accepted, but not committed
              String ballotString = BallotGenerator.Global.nextBallot(Flag.GLOBAL).toString();
              saveAcceptedPaxosProposal(tableName, ballotString, migratingKey);
              // PaxosRepair will have inserted a condition matching row, so it can apply, demonstrating repair and
              // key migration occurred
              assertTargetAccordWrite(runCasApplies, 1, migratingKey, 1, 0, 1, 0, 0);

              // This will force the request to run on Paxos up to Accept
              // and the accept will be rejected at both nodes and we are certain we need to retry the transaction
              cluster.get(1).runOnInstance(() -> ConsensusRequestRouter.setInstance(new PaxosToAccordMigrationNotHappeningUpToBegin()));
              // Update inserted row so the condition can apply, if the condition check doesn't apply
              // then it won't get to propose/accept
              migratingKey = testingKeys.next();
              Consumer<Integer> makeCASApply = key -> cluster.coordinator(1).execute("UPDATE " + qualifiedTableName + " SET v = 42 WHERE id = ? AND c = ?", ALL, key, CLUSTERING_VALUE);
              makeCASApply.accept(migratingKey);
              assertTargetAccordWrite(runCasApplies, 1, migratingKey, 1, 1, 1, 0, 1);

              // One node will now accept the other will reject and we are uncertain if we should retry the transaction
              // and should surface that as a timeout exception
              migratingKey = testingKeys.next();
              makeCASApply.accept(migratingKey);
              cluster.get(1).runOnInstance(() -> ConsensusRequestRouter.setInstance(new PaxosToAccordMigrationNotHappeningUpToAccept()));
              try
              {
                  runCasNoApply.accept(migratingKey);
                  fail("Should have thrown timeout exception");
              }
              catch (Throwable t)
              {
                  if (!t.getClass().getName().equals("org.apache.cassandra.exceptions.CasWriteTimeoutException"))
                      throw new RuntimeException(t);
              }

              // Test that if we find out about a migration from the prepare phase Paxos.begin we
              // retry it on Accord
              cluster.get(1).runOnInstance(() -> ConsensusRequestRouter.setInstance(new RoutesToPaxosOnce()));
              // Should exit Paxos from begin, key migration should occur because it's a new key, and Accord will need to do a migration read
              assertTargetAccordWrite(runCasNoApply, 1, testingKeys.next(), 1, 1, 1, 1, 0);

              // Now do two repairs to complete the migration repair, and we are done with black box integration testing
              // First repair is a range smack dab in the middle
              Token startTokenForRepair = partitioner.midpoint(lowerMidToken, midToken);
              Token endTokenForRepair = partitioner.midpoint(startTokenForRepair, midToken);
              nodetool(coordinator, "consensus_admin", "finish-migration", "-st", startTokenForRepair.toString(), "-et", endTokenForRepair.toString());
              List<Range<Token>> migratedRanges = ImmutableList.of(new Range<>(startTokenForRepair, endTokenForRepair), migratingRange);
              List<Range<Token>> midMigratingRanges = ImmutableList.of(new Range<>(lowerMidToken, startTokenForRepair), new Range<>(endTokenForRepair, midToken));
              List<Range<Token>> migratingAndMigratedRanges = ImmutableList.of(new Range<>(lowerMidToken, maxToken));
              assertMigrationState(tableName, ConsensusMigrationTarget.accord, migratedRanges, midMigratingRanges, 1);

              nodetool(coordinator, "consensus_admin", "finish-migration");
              assertMigrationState(tableName, ConsensusMigrationTarget.accord, migratingAndMigratedRanges, emptyList(), 0);
          });
    }

    /*
     * Read has a few code paths that are separate from CAS that need to be tested
     * such as switching consensus protocol, rejecting read during accept, and throwing
     * timeout exception if uncertain about side effects
     */
    @Test
    public void testPaxosToAccordSerialRead() throws Exception
    {
        test(format(TABLE_FMT, qualifiedTableName),
          cluster -> {
              String tableName = qualifiedTableName.split("\\.")[1];
              String readCQL = format("SELECT * FROM %s WHERE id = ? and c = %s", qualifiedTableName, CLUSTERING_VALUE);
              Function<Integer, Object[][]> runRead = key -> cluster.coordinator(1).execute(readCQL, SERIAL, key);
              Range<Token> migratingRange = new Range<>(new LongToken(Long.MIN_VALUE + 1), new LongToken(Long.MIN_VALUE));
              List<Range<Token>> migratingRanges = ImmutableList.of(migratingRange);
              int key = 0;

              assertTargetAccordRead(runRead, 1, 0, 0, 1, 0, 0, 0);
              // Mark wrap around range as migrating
              nodetool(coordinator, "consensus_admin", "begin-migration", "-st", String.valueOf(Long.MIN_VALUE + 1), "-et", String.valueOf(Long.MIN_VALUE), "-tp", "accord", KEYSPACE, tableName);
              assertMigrationState(tableName, ConsensusMigrationTarget.accord, emptyList(), migratingRanges, 1);
              // Should run directly on accord, migrate the key, and perform a quorum read from Accord, Paxos repair will run prepare once
              assertTargetAccordRead(runRead, 1, key++, 1, 1, 1, 0, 0);

              // Should run up to accept with both nodes refusing to accept
              savePromisedAndCommittedPaxosProposal(tableName, key);
              cluster.get(1).runOnInstance(() -> ConsensusRequestRouter.setInstance(new PaxosToAccordMigrationNotHappeningUpToBegin()));
              assertTargetAccordRead(runRead, 1, key++, 1, 2, 1, 0, 1);
          });
    }

    @Test
    public void testAccordToPaxos() throws Exception
    {
        test(format(TABLE_FMT, qualifiedTableName),
             cluster -> {
                 String casCQL = format(CAS_FMT, qualifiedTableName, CLUSTERING_VALUE);
                 Consumer<Integer> runCasNoApply = key -> assertRowEquals(cluster, new Object[]{false}, casCQL, key);
                 String tableName = qualifiedTableName.split("\\.")[1];

                 // Mark a subrange as migrating and finish migrating half of it
                 nodetool(coordinator, "consensus_admin", "begin-migration", "-st", midToken.toString(), "-et", maxToken.toString(), "-tp", "accord", KEYSPACE, tableName);
                 nodetool(coordinator, "consensus_admin", "finish-migration", "-st", midToken.toString(), "-et", "3074457345618258601");
                 nodetool(coordinator, "consensus_admin", "finish-migration", "-st", "3074457345618258601", "-et", upperMidToken.toString());
                 Range<Token> accordMigratedRange = new Range(midToken, upperMidToken);
                 Range<Token> accordMigratingRange = new Range(upperMidToken, maxToken);
                 assertMigrationState(tableName, ConsensusMigrationTarget.accord, ImmutableList.of(accordMigratedRange), ImmutableList.of(accordMigratingRange), 1);

                 // Test that we can reverse the migration and go back to Paxos
                 nodetool(coordinator, "consensus_admin", "set-target-protocol", "-tp", "paxos", KEYSPACE, tableName);
                 assertMigrationState(tableName, ConsensusMigrationTarget.paxos, ImmutableList.of(new Range(minToken, midToken), new Range(maxToken, minToken)), ImmutableList.of(accordMigratingRange), 1);
                 Iterator<Integer> paxosNonMigratingKeys = getKeysBetweenTokens(minToken, midToken);
                 Iterator<Integer> paxosMigratingKeys = getKeysBetweenTokens(upperMidToken, maxToken);
                 Iterator<Integer> accordKeys = getKeysBetweenTokens(midToken, upperMidToken);

                 // Paxos non-migrating keys should run on Paxos as per normal
                 assertTargetPaxosWrite(runCasNoApply, 1, paxosNonMigratingKeys.next(), 0, 1, 0, 0, 0);

                 // Paxos migrating keys should be key migrated which means a local barrier is run by Paxos during read at each replica, the key migration barrier is also counted as a write
                 assertTargetPaxosWrite(runCasNoApply, 1, paxosMigratingKeys.next(), 1, 1, 1, 0, 0);

                 // A key from a range migrated to Accord is now not migrating/migrated and should be accessed through Accord
                 assertTargetPaxosWrite(runCasNoApply, 1, accordKeys.next(), 1, 0, 0, 0, 0);

                 // If an Accord transaction races with cluster metadata updates it should be rejected if the epoch it runs in contains the migration
                 cluster.get(1).runOnInstance(() -> ConsensusRequestRouter.setInstance(new RoutesToAccordOnce()));
                 assertTargetPaxosWrite(runCasNoApply, 1, paxosMigratingKeys.next(), 2, 1, 1, 1, 1);

                 // Repair the currently migrating range from when targets were switched, but it's not an Accord repair, this is to make sure the wrong repair type doesn't trigger progress
                 nodetool(coordinator, "repair", "-st", upperMidToken.toString(), "-et", maxAlignedWithLocalRanges.toString());
                 assertMigrationState(tableName, ConsensusMigrationTarget.paxos, ImmutableList.of(new Range(minToken, midToken), new Range(maxToken, minToken)), ImmutableList.of(accordMigratingRange), 1);

                 // Paxos migrating keys should still need key migration after non-Accord repair
                 assertTargetPaxosWrite(runCasNoApply, 1, paxosMigratingKeys.next(), 1, 1, 1, 0, 0);

                 // Now do it with an Accord repair so key migration shouldn't be necessary
                 nodetool(coordinator, "consensus_admin", "finish-migration", "-st", upperMidToken.toString(), "-et", maxAlignedWithLocalRanges.toString());
                 Range<Token> repairedRange = new Range(upperMidToken, maxAlignedWithLocalRanges);
                 // Sliver remaining because of precise repairs
                 // TODO This precision isn't needed for Accord repair? Worth lifting that restriction or keep it consistent?
                 Range<Token> remainingRange = new Range(maxAlignedWithLocalRanges, maxToken);
                 assertMigrationState(tableName, ConsensusMigrationTarget.paxos, ImmutableList.of(new Range(minToken, midToken), repairedRange, new Range(maxToken, minToken)), ImmutableList.of(remainingRange), 1);

                 // Paxos migrating keys shouldn't need key migration after Accord repair
                 assertTargetPaxosWrite(runCasNoApply, 1, paxosMigratingKeys.next(), 0, 1, 0, 0, 0);
             });
    }

    private static void assertMigrationState(String tableName, ConsensusMigrationTarget target, List<Range<Token>> migratedRanges, List<Range<Token>> migratingRanges, int numMigratingEpochs) throws Throwable
    {
        // Validate nodetool consensus admin list output
        String yamlResultString = nodetool(SHARED_CLUSTER.coordinator(1), "consensus_admin", "list");
        Map<String, Object> yamlStateMap = new Yaml().load(yamlResultString);
        String minifiedYamlResultString = nodetool(SHARED_CLUSTER.coordinator(1), "consensus_admin", "list", "-f", "minified-yaml");
        Map<String, Object> minifiedYamlStateMap = new Yaml().load(minifiedYamlResultString);
        String jsonResultString = nodetool(SHARED_CLUSTER.coordinator(1), "consensus_admin", "list", "-f", "json");
        Map<String, Object> jsonStateMap = JsonUtils.JSON_OBJECT_MAPPER.readValue(jsonResultString, new TypeReference<Map<String, Object>>(){});
        String minifiedJsonResultString = nodetool(SHARED_CLUSTER.coordinator(1), "consensus_admin", "list", "-f", "minified-json");
        Map<String, Object> minifiedJsonStateMap = JsonUtils.JSON_OBJECT_MAPPER.readValue(minifiedJsonResultString, new TypeReference<Map<String, Object>>(){});

        List<String> tableIds = new ArrayList<>();
        for (Map<String, Object> migrationStateMap : ImmutableList.of(yamlStateMap, jsonStateMap, minifiedYamlStateMap, minifiedJsonStateMap))
        {
            assertEquals(PojoToString.CURRENT_VERSION, migrationStateMap.get("version"));
            assertTrue(Epoch.EMPTY.getEpoch() < ((Number) migrationStateMap.get("lastModifiedEpoch")).longValue());
            List<Map<String, Object>> tableStates = (List<Map<String, Object>>) migrationStateMap.get("tableStates");
            assertEquals(tableStates.size(), 1);
            Map<String, Object> tableStateMap = tableStates.get(0);
            assertEquals(tableName, tableStateMap.get("table"));
            assertEquals(KEYSPACE, tableStateMap.get("keyspace"));
            tableIds.add((String) tableStateMap.get("tableId"));
            List<Range<Token>> migratedRangesFromStateMap = ((List<String>) tableStateMap.get("migratedRanges")).stream().map(Range::fromString).collect(toImmutableList());
            assertEquals(migratedRanges, migratedRangesFromStateMap);
            Map<Long, List<Range<Token>>> migratingRangesByEpochFromStateMap = new LinkedHashMap<>();
            for (Map.Entry<Object, List<String>> entry : ((Map<Object, List<String>>) tableStateMap.get("migratingRangesByEpoch")).entrySet())
            {
                long epoch = entry.getKey() instanceof Number ? ((Number)entry.getKey()).longValue() : Long.valueOf((String)entry.getKey());
                migratingRangesByEpochFromStateMap.put(epoch, entry.getValue().stream().map(Range::fromString).collect(toImmutableList()));
            }
            if (migratingRanges.isEmpty())
                assertEquals(0, migratingRangesByEpochFromStateMap.size());
            else
                assertEquals(migratingRanges, migratingRangesByEpochFromStateMap.values().iterator().next());
        }

        // Also check JSON format at least loads without error
        // Validate in memory state at each node
        List<Range<Token>> migratingAndMigratedRanges = normalize(ImmutableList.<Range<Token>>builder().addAll(migratedRanges).addAll(migratingRanges).build());
        spinUntilSuccess(() -> {
            for (IInvokableInstance instance : SHARED_CLUSTER)
            {
                ConsensusMigrationState snapshot = getMigrationStateSnapshot(instance);
                assertEquals(1, snapshot.tableStates.size());
                TableMigrationState state = snapshot.tableStates.values().iterator().next();
                assertEquals(KEYSPACE, state.keyspaceName);
                assertEquals(tableName, state.tableName);
                for (String tableId : tableIds)
                    assertEquals(tableId, state.tableId.toString());
                assertEquals(target, state.targetProtocol);
                assertEquals("Migrated ranges:", migratedRanges, state.migratedRanges);
                assertEquals("Migrating ranges:", migratingRanges, state.migratingRanges);
                assertEquals("Migrating and migrated ranges:", migratingAndMigratedRanges, state.migratingAndMigratedRanges);
                assertEquals(numMigratingEpochs, state.migratingRangesByEpoch.size());
                if (migratingRanges.isEmpty())
                    assertEquals(0, state.migratingRangesByEpoch.size());
                else
                    assertEquals(migratingRanges, state.migratingRangesByEpoch.values().iterator().next());
            }
        });
    }

    /**
     * Save a promise that is after the committed one to make a subsequent read not linearizable
     */
    private static void savePromisedAndCommittedPaxosProposal(String tableName, int key)
    {
        String committedBallotString = BallotGenerator.Global.nextBallot(Flag.GLOBAL).toString();
        String promisedBallotString = BallotGenerator.Global.nextBallot(Flag.GLOBAL).toString();
        forEach(() -> {
            TableMetadata metadata = ColumnFamilyStore.getIfExists(KEYSPACE, tableName).metadata();
            ByteBuffer lowMidMigratingKeyBuffer = ByteBuffer.wrap(ByteArrayUtil.bytes(key));
            DecoratedKey dk = new BufferDecoratedKey(DatabaseDescriptor.getPartitioner().getToken(lowMidMigratingKeyBuffer), lowMidMigratingKeyBuffer);
            try (PaxosState state = PaxosState.get(dk, metadata))
            {
                Ballot ballot = Ballot.fromString(committedBallotString);
                PartitionUpdateBuilder updateBuilder = new PartitionUpdateBuilder(metadata, key);
                updateBuilder.row(CLUSTERING_VALUE).add("v", 42);

                state.commit(new Agreed(ballot, updateBuilder.build()));
                state.promiseIfNewer(Ballot.fromString(promisedBallotString), true);
            }
        });
    }

    private static void saveAcceptedPaxosProposal(String tableName, String ballotString, int key)
    {
        forEach(() -> {
            TableMetadata metadata = ColumnFamilyStore.getIfExists(KEYSPACE, tableName).metadata();
            ByteBuffer lowMidMigratingKeyBuffer = ByteBuffer.wrap(ByteArrayUtil.bytes(key));
            DecoratedKey dk = new BufferDecoratedKey(DatabaseDescriptor.getPartitioner().getToken(lowMidMigratingKeyBuffer), lowMidMigratingKeyBuffer);
            try (PaxosState state = PaxosState.get(dk, metadata))
            {
                Ballot ballot = Ballot.fromString(ballotString);
                assertEquals( PROMISE, state.promiseIfNewer(ballot, true).outcome());
                PartitionUpdateBuilder updateBuilder = new PartitionUpdateBuilder(metadata, key);
                updateBuilder.row(CLUSTERING_VALUE).add("v", 42);
                // Set isForRepair to true to force accepting the proposal for testing purposes
                assertEquals( null, state.acceptIfLatest(new Proposal(ballot, updateBuilder.build()), true).supersededBy);
            }
        });
    }
}
