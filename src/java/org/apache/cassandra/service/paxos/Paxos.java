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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InOurDc;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.ReplicaLayout.ForTokenWrite;
import org.apache.cassandra.locator.ReplicaPlan.ForRead;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.metrics.ClientRequestSizeMetrics;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.FailureRecordingCallback.AsMap;
import org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter;
import org.apache.cassandra.service.paxos.Commit.Proposal;
import org.apache.cassandra.service.paxos.PaxosPrepare.FoundIncompleteAccepted;
import org.apache.cassandra.service.paxos.PaxosPrepare.FoundIncompleteCommitted;
import org.apache.cassandra.service.paxos.PaxosPropose.Superseded;
import org.apache.cassandra.service.paxos.cleanup.PaxosRepairState;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.service.reads.repair.NoopReadRepair;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.PAXOS_LOG_TTL_LINEARIZABILITY_VIOLATIONS;
import static org.apache.cassandra.config.CassandraRelevantProperties.PAXOS_MODERN_RELEASE;
import static org.apache.cassandra.config.Config.PaxosVariant.v2_without_linearizable_reads_or_rejected_writes;
import static org.apache.cassandra.config.DatabaseDescriptor.getCasContentionTimeout;
import static org.apache.cassandra.config.DatabaseDescriptor.getWriteRpcTimeout;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_SERIAL;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.db.Keyspace.openAndGetStore;
import static org.apache.cassandra.exceptions.RequestFailureReason.TIMEOUT;
import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;
import static org.apache.cassandra.locator.ReplicaLayout.forTokenWriteLiveAndDown;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.casReadMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.casWriteMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.readMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.readMetricsMap;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.writeMetricsMap;
import static org.apache.cassandra.service.StorageProxy.ConsensusAttemptResult;
import static org.apache.cassandra.service.StorageProxy.ConsensusAttemptResult.RETRY_NEW_PROTOCOL;
import static org.apache.cassandra.service.StorageProxy.ConsensusAttemptResult.casResult;
import static org.apache.cassandra.service.StorageProxy.ConsensusAttemptResult.serialReadResult;
import static org.apache.cassandra.service.paxos.Ballot.Flag.GLOBAL;
import static org.apache.cassandra.service.paxos.Ballot.Flag.LOCAL;
import static org.apache.cassandra.service.paxos.BallotGenerator.Global.nextBallot;
import static org.apache.cassandra.service.paxos.BallotGenerator.Global.staleBallot;
import static org.apache.cassandra.service.paxos.ContentionStrategy.Type.READ;
import static org.apache.cassandra.service.paxos.ContentionStrategy.Type.WRITE;
import static org.apache.cassandra.service.paxos.ContentionStrategy.waitForContention;
import static org.apache.cassandra.service.paxos.PaxosCommit.commit;
import static org.apache.cassandra.service.paxos.PaxosCommitAndPrepare.commitAndPrepare;
import static org.apache.cassandra.service.paxos.PaxosPrepare.Status.Outcome.MAYBE_FAILURE;
import static org.apache.cassandra.service.paxos.PaxosPrepare.prepare;
import static org.apache.cassandra.service.paxos.PaxosPropose.propose;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeSet;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;
import static org.apache.cassandra.utils.NoSpamLogger.Level.WARN;

/**
 * <p>This class serves as an entry-point to Cassandra's implementation of Paxos Consensus.
 * Note that Cassandra does not utilise the distinguished proposer (Multi Paxos) optimisation;
 * each operation executes its own instance of Paxos Consensus. Instead Cassandra employs
 * various optimisations to reduce the overhead of operations. This may lead to higher throughput
 * and lower overhead read operations, at the expense of contention during mixed or write-heavy workloads.
 *
 * Firstly, note that we do not follow Lamport's formulation, instead following the more common approach in
 * literature (see e.g. Dr. Heidi Howard's dissertation) of permitting any acceptor to vote on a proposal,
 * not only those who issued a promise.
 *
 * <h2>No Commit of Empty Proposals</h2>
 * <p>If a proposal is empty, there can be no effect to the state, so once this empty proposal has poisoned any earlier
 * proposal it is safe to stop processing. An empty proposal effectively scrubs the instance of consensus being
 * performed once it has reached a quorum, as no earlier incomplete proposal (that may perhaps have reached a minority)
 * may now be completed.
 *
 * <h2>Fast Read / Failed Write</h2>
 * <p>This optimisation relies on every voter having no incomplete promises, i.e. their commit register must be greater
 * than or equal to their promise and proposal registers (or there must be such an empty proposal).
 * Since the operation we are performing must invalidate any nascent operation that has reached a minority, and will
 * itself be invalidated by any newer write it might race with, we are only concerned about operations that might be
 * in-flight and incomplete. If we reach a quorum without any incomplete proposal, we prevent any incomplete proposal
 * that might have come before us from being committed, and so are correctly ordered.
 *
 * <p>NOTE: we could likely weaken this further, permitting a fast operation if we witness a stale incomplete operation
 * on one or more of the replicas, so long as we witness _some_ response that had knowledge of that operation's decision,
 * however we might waste more time performing optimistic reads (which we skip if we witness any in progress promise)
 *
 * <h2>Read Commutativity Optimisation</h2>
 * <p>We separate read and write promises into distinct registers. Since reads are commutative they do not need to be
 * ordered with respect to each other, so read promises consult only the write promise register to find competing
 * operations, whereas writes consult both read and write registers. This permits better utilisation of the Fast Read
 * optimisation, permitting arbitrarily many fast reads to execute concurrently.
 *
 * <p>A read will use its promise to finish any in progress write it encounters, but note that this is safe for multiple
 * reads to attempt simultaneously. If a write operation has not reached a quorum of promises then it has no effect,
 * so while some read operations may attempt to complete it and others may not, the operation will only be invalidated
 * and these actions will be equivalent. If the write had reached a quorum of promises then every reads will attempt
 * to complete the write. At the accept phase, only the most recent read promise will be accepted so whether the write
 * proposal had reached a quorum or not, a consistent outcome will result.
 *
 * <h2>Reproposal Avoidance</h2>
 * <p>It can occur that two (or more) commands begin competing to re-propose the same incomplete command even after it
 * has already committed - this can occur when an in progress command that has reached the commit condition (but not yet
 * committed) is encountered by a promise, so that it is re-proposed. If the original coordinator does not fail this
 * original command will be committed normally, but the re-proposal can take on a life of its own, and become contended
 * and re-proposed indefinitely. By having reproposals use the original proposal ballot's timestamp we spot this situation
 * and consider re-proposals of a command we have seen committed to be (in effect) empty proposals.
 *
 * <h2>Durability of Asynchronous Commit</h2>
 * To permit asynchronous commit (and also because we should) we ensure commits are durable once a proposal has been
 * accepted by a majority.
 *
 * Replicas track commands that have *locally* been witnessed but not committed. They may clear this log by performing
 * a round of Paxos Repair for each key in the log (which is simply a round of Paxos that tries not to interfere with
 * future rounds of Paxos, while aiming to complete any earlier incomplete round).
 *
 * By selecting some quorum of replicas for a range to perform this operation on, once successful we guarantee that
 * any transaction that had previously been accepted by a majority has been committed, and any transaction that had been
 * previously witnessed by a majority has been either committed or invalidated.
 *
 * To ensure durability across range movements, once a joining node becomes pending such a coordinated paxos repair
 * is performed prior to performing bootstrap, so that commands initiated before joining will either be bootstrapped
 * or completed by paxos repair to be committed to a majority that includes the new node in its calculations, and
 * commands initiated after will anyway do so due to being pending.
 *
 * Finally, for greater guarantees across range movements despite the uncertainty of gossip, paxos operations validate
 * ring information with each other while seeking a quorum of promises. Any inconsistency is resolved by synchronising
 * gossip state between the coordinator and the peers in question.
 *
 * <h2>Clearing of Paxos State</h2>
 * Coordinated paxos repairs as described above are preceded by an preparation step that determines a ballot below
 * which we agree to reject new promises. By deciding and disseminating this point prior to performing a coordinated
 * paxos repair, once complete we have ensured that all commands with a lower ballot are either committed or invalidated,
 * and so we are then able to disseminate this ballot as a bound below which may expunge all data for the range.
 *
 * For consistency of execution coordinators seek this latter ballot bound from each replica and, using the maximum of
 * these, ignore all data received associated with ballots lower than this bound.
 */
public class Paxos
{
    private static final Logger logger = LoggerFactory.getLogger(Paxos.class);

    private static volatile Config.PaxosVariant PAXOS_VARIANT = DatabaseDescriptor.getPaxosVariant();
    private static final CassandraVersion MODERN_PAXOS_RELEASE = new CassandraVersion(PAXOS_MODERN_RELEASE.getString());
    static final boolean LOG_TTL_LINEARIZABILITY_VIOLATIONS = PAXOS_LOG_TTL_LINEARIZABILITY_VIOLATIONS.getBoolean();

    static class Electorate implements Iterable<InetAddressAndPort>
    {
        static final Serializer serializer = new Serializer();

        // all replicas, including pending, but without those in a remote DC if consistency is local
        final Collection<InetAddressAndPort> natural;

        // pending subset of electorate
        final Collection<InetAddressAndPort> pending;

        public Electorate(Collection<InetAddressAndPort> natural, Collection<InetAddressAndPort> pending)
        {
            this.natural = natural;
            this.pending = pending;
        }

        public int size()
        {
            return natural.size() + pending.size();
        }

        @Override
        public Iterator<InetAddressAndPort> iterator()
        {
            return Iterators.concat(natural.iterator(), pending.iterator());
        }

        static Electorate.Local get(TableMetadata table, DecoratedKey key, ConsistencyLevel consistency)
        {
            // MetaStrategy distributes the entire keyspace to all replicas. In addition, its tables (currently only
            // the dist log table) don't use the globally configured partitioner. For these reasons we don't lookup the
            // replicas using the supplied token as this can actually be of the incorrect type (for example when
            // performing Paxos repair).
            final Token token = table.partitioner == MetaStrategy.partitioner ? MetaStrategy.entireRange.right : key.getToken();
            ClusterMetadata metadata = ClusterMetadata.current();
            Keyspace keyspace = Keyspace.open(table.keyspace);
            DataPlacement placement = metadata.placements.get(keyspace.getMetadata().params.replication);
            Epoch epoch = placement.writes.forToken(token).lastModified();
            ForTokenWrite electorate = forTokenWriteLiveAndDown(metadata, keyspace, token);
            if (consistency == LOCAL_SERIAL)
                electorate = electorate.filter(InOurDc.replicas());

            return new Local(epoch, electorate.natural().endpointList(), electorate.pending().endpointList());
        }

        boolean hasPending()
        {
            return !pending.isEmpty();
        }

        boolean isPending(InetAddressAndPort endpoint)
        {
            return hasPending() && pending.contains(endpoint);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || !Electorate.class.isAssignableFrom(o.getClass())) return false;
            Electorate that = (Electorate) o;
            return natural.size() == that.natural.size() &&
                   pending.size() == that.pending.size() &&
                   natural.containsAll(that.natural)  &&
                   pending.containsAll(that.pending);
        }

        public int hashCode()
        {
            return Objects.hash(natural, pending);
        }

        public String toString()
        {
            return "{" + natural + ", " + pending + '}';
        }

        static class Serializer implements IVersionedSerializer<Electorate>
        {
            public void serialize(Electorate electorate, DataOutputPlus out, int version) throws IOException
            {
                serializeCollection(electorate.natural, out, version, inetAddressAndPortSerializer);
                serializeCollection(electorate.pending, out, version, inetAddressAndPortSerializer);
            }

            public Electorate deserialize(DataInputPlus in, int version) throws IOException
            {
                Set<InetAddressAndPort> endpoints = deserializeSet(in, version, inetAddressAndPortSerializer);
                Set<InetAddressAndPort> pending = deserializeSet(in, version, inetAddressAndPortSerializer);
                return new Electorate(endpoints, pending);
            }

            public long serializedSize(Electorate electorate, int version)
            {
                return serializedCollectionSize(electorate.natural, version, inetAddressAndPortSerializer) +
                       serializedCollectionSize(electorate.pending, version, inetAddressAndPortSerializer);
            }
        }

        static class Local extends Electorate
        {
            final Epoch createdAt;
            public Local(Epoch createdAt, Collection<InetAddressAndPort> natural, Collection<InetAddressAndPort> pending)
            {
                super(natural, pending);
                this.createdAt = createdAt;
            }
        }
    }

    /**
     * Encapsulates the peers we will talk to for this operation.
     */
    static class Participants implements ForRead<EndpointsForToken, Participants>
    {
        final Keyspace keyspace;

        final AbstractReplicationStrategy replicationStrategy;

        /**
         * SERIAL or LOCAL_SERIAL
         */
        final ConsistencyLevel consistencyForConsensus;

        /**
         * Those members that vote for {@link #consistencyForConsensus}
         */
        final Electorate electorate;

        /**
         * Those members of {@link #electorate} that we will 'poll' for their vote
         * i.e. {@link #electorate} with down nodes removed
         */

        private final EndpointsForToken electorateNatural;
        final EndpointsForToken electorateLive;

        final EndpointsForToken all;
        final EndpointsForToken allLive;
        final EndpointsForToken allDown;
        final EndpointsForToken pending;

        /**
         * The number of responses we require to reach desired consistency from members of {@code contact}
         */
        final int sizeOfConsensusQuorum;

        /**
         * The number of read responses we require to reach desired consistency from members of {@code contact}
         * Note that this should always be met if {@link #sizeOfConsensusQuorum} is met, but we supply it separately
         * for corroboration.
         */
        final int sizeOfReadQuorum;

        final Epoch epoch;
        final Function<ClusterMetadata, Participants> recompute;

        Participants(Epoch epoch, Keyspace keyspace, ConsistencyLevel consistencyForConsensus, ReplicaLayout.ForTokenWrite all, ReplicaLayout.ForTokenWrite electorate, EndpointsForToken live,
                     Function<ClusterMetadata, Participants> recompute)
        {
            this.epoch = epoch;
            this.keyspace = keyspace;
            this.replicationStrategy = all.replicationStrategy();
            this.consistencyForConsensus = consistencyForConsensus;
            this.all = all.all();
            this.pending = all.pending();
            this.allDown = all.all() == live ? EndpointsForToken.empty(all.token()) : all.all().without(live.endpoints());
            this.electorate = new Electorate(electorate.natural().endpointList(), electorate.pending().endpointList());
            this.electorateNatural = electorate.natural();
            this.electorateLive = electorate.all() == live ? live : electorate.all().keep(live.endpoints());
            this.allLive = live;
            this.sizeOfReadQuorum = electorate.natural().size() / 2 + 1;
            this.sizeOfConsensusQuorum = sizeOfReadQuorum + electorate.pending().size();
            this.recompute = recompute;
        }

        @Override
        public int readQuorum()
        {
            return sizeOfReadQuorum;
        }

        @Override
        public EndpointsForToken readCandidates()
        {
            // Note: we could probably return electorateLive here and save a reference, but it's not strictly correct
            return electorateNatural;
        }

        @Override
        public boolean stillAppliesTo(ClusterMetadata newMetadata)
        {
            if (newMetadata.epoch.equals(epoch))
                return true;

            Participants newParticipants = recompute.apply(newMetadata);
            return newParticipants.electorate.equals(electorate);
        }

        @Override
        public void collectSuccess(InetAddressAndPort inetAddressAndPort)
        {

        }

        @Override
        public void collectFailure(InetAddressAndPort inetAddressAndPort, RequestFailure t)
        {

        }

        static Participants get(ClusterMetadata metadata, TableMetadata table, Token token, ConsistencyLevel consistencyForConsensus)
        {
            return get(metadata, table, token, consistencyForConsensus, FailureDetector.isReplicaAlive);
        }

        static Participants get(ClusterMetadata metadata, TableMetadata table, Token token, ConsistencyLevel consistencyForConsensus, Predicate<Replica> isReplicaAlive)
        {
            KeyspaceMetadata keyspaceMetadata = metadata.schema.getKeyspaceMetadata(table.keyspace);
            // MetaStrategy distributes the entire keyspace to all replicas. In addition, its tables (currently only
            // the dist log table) don't use the globally configured partitioner. For these reasons we don't lookup the
            // replicas using the supplied token as this can actually be of the incorrect type (for example when
            // performing Paxos repair).
            final Token actualToken = table.partitioner == MetaStrategy.partitioner ? MetaStrategy.entireRange.right : token;
            ReplicaLayout.ForTokenWrite all = forTokenWriteLiveAndDown(keyspaceMetadata, actualToken);
            ReplicaLayout.ForTokenWrite electorate = consistencyForConsensus.isDatacenterLocal()
                                                     ? all.filter(InOurDc.replicas()) : all;

            EndpointsForToken live = all.all().filter(isReplicaAlive);
            return new Participants(metadata.epoch, Keyspace.open(table.keyspace), consistencyForConsensus, all, electorate, live,
                                    (cm) -> get(cm, table, actualToken, consistencyForConsensus));
        }

        static Participants get(TableMetadata table, Token token, ConsistencyLevel consistencyForConsensus)
        {
            return get(ClusterMetadata.current(), table, token, consistencyForConsensus);
        }

        static Participants get(TableMetadata cfm, DecoratedKey key, ConsistencyLevel consistency)
        {
            return get(cfm, key.getToken(), consistency);
        }

        int sizeOfPoll()
        {
            return electorateLive.size();
        }

        InetAddressAndPort voter(int i)
        {
            return electorateLive.endpoint(i);
        }

        void assureSufficientLiveNodes(boolean isWrite) throws UnavailableException
        {
            if (sizeOfConsensusQuorum > sizeOfPoll())
            {
                mark(isWrite, m -> m.unavailables, consistencyForConsensus);
                throw new UnavailableException("Cannot achieve consistency level " + consistencyForConsensus + " " + sizeOfConsensusQuorum + " > " + sizeOfPoll(), consistencyForConsensus, sizeOfConsensusQuorum, sizeOfPoll());
            }
        }

        void assureSufficientLiveNodesForRepair() throws UnavailableException
        {
            if (sizeOfConsensusQuorum > sizeOfPoll())
            {
                throw UnavailableException.create(consistencyForConsensus, sizeOfConsensusQuorum, sizeOfPoll());
            }
        }

        int requiredFor(ConsistencyLevel consistency)
        {
            if (consistency == Paxos.nonSerial(consistencyForConsensus))
                return sizeOfConsensusQuorum;

            return consistency.blockForWrite(replicationStrategy(), pending);
        }

        public boolean hasOldParticipants()
        {
            return electorateLive.anyMatch(Paxos::isOldParticipant);
        }

        public Epoch epoch()
        {
            return epoch;
        }

        @Override
        public Keyspace keyspace()
        {
            return keyspace;
        }

        @Override
        public AbstractReplicationStrategy replicationStrategy()
        {
            return replicationStrategy;
        }

        @Override
        public ConsistencyLevel consistencyLevel()
        {
            return nonSerial(consistencyForConsensus);
        }

        @Override
        public EndpointsForToken contacts()
        {
            return electorateLive;
        }

        @Override
        public Replica lookup(InetAddressAndPort endpoint)
        {
            return all.lookup(endpoint);
        }

        @Override
        public Participants withContacts(EndpointsForToken newContacts)
        {
            throw new UnsupportedOperationException();
        }

        public boolean isUrgent()
        {
            return keyspace.getMetadata().params.replication.isMeta();
        }
    }

    /**
     * Encapsulates information about a failure to reach Success, either because of explicit failure responses
     * or insufficient responses (in which case the status is not final)
     */
    static class MaybeFailure
    {
        final boolean isFailure;
        final String serverError;
        final int contacted;
        final int required;
        final int successes;
        final Map<InetAddressAndPort, RequestFailureReason> failures;

        static MaybeFailure noResponses(Participants contacted)
        {
            return new MaybeFailure(false, contacted.sizeOfPoll(), contacted.sizeOfConsensusQuorum, 0, emptyMap());
        }

        MaybeFailure(Participants contacted, int successes, AsMap failures)
        {
            this(contacted.sizeOfPoll() - failures.failureCount() < contacted.sizeOfConsensusQuorum, contacted.sizeOfPoll(), contacted.sizeOfConsensusQuorum, successes, failures);
        }

        MaybeFailure(int contacted, int required, int successes, AsMap failures)
        {
            this(contacted - failures.failureCount() < required, contacted, required, successes, failures);
        }

        MaybeFailure(boolean isFailure, int contacted, int required, int successes, Map<InetAddressAndPort, RequestFailureReason> failures)
        {
            this(isFailure, null, contacted, required, successes, failures);
        }

        MaybeFailure(boolean isFailure, String serverError, int contacted, int required, int successes, Map<InetAddressAndPort, RequestFailureReason> failures)
        {
            this.isFailure = isFailure;
            this.serverError = serverError;
            this.contacted = contacted;
            this.required = required;
            this.successes = successes;
            this.failures = failures;
        }

        private static int failureCount(Map<InetAddressAndPort, RequestFailureReason> failures)
        {
            int count = 0;
            for (RequestFailureReason reason : failures.values())
                count += reason != TIMEOUT ? 1 : 0;
            return count;
        }

        /**
         * update relevant counters and throw the relevant exception
         */
        RequestExecutionException markAndThrowAsTimeoutOrFailure(boolean isWrite, ConsistencyLevel consistency, int failedAttemptsDueToContention)
        {
            if (isFailure)
            {
                mark(isWrite, m -> m.failures, consistency);
                throw serverError != null ? new RequestFailureException(ExceptionCode.SERVER_ERROR, serverError, consistency, successes, required, failures)
                                          : isWrite
                                            ? new WriteFailureException(consistency, successes, required, WriteType.CAS, failures)
                                            : new ReadFailureException(consistency, successes, required, false, failures);
            }
            else
            {
                mark(isWrite, m -> m.timeouts, consistency);
                throw isWrite
                        ? new CasWriteTimeoutException(WriteType.CAS, consistency, successes, required, failedAttemptsDueToContention)
                        : new ReadTimeoutException(consistency, successes, required, false);
            }
        }

        public String toString()
        {
            return (isFailure ? "Failure(" : "Timeout(") + successes + ',' + failures + ')';
        }
    }

    public interface Async<Result>
    {
        Result awaitUntil(long until);
    }

    /**
     * Apply @param updates if and only if the current values in the row for @param key
     * match the provided @param conditions.  The algorithm is "raw" Paxos: that is, Paxos
     * minus leader election -- any node in the cluster may propose changes for any partition.
     *
     * The Paxos electorate consists only of the replicas for the partition key.
     * We expect performance to be reasonable, but CAS is still intended to be used
     * "when you really need it," not for all your updates.
     *
     * There are three phases to Paxos:
     *  1. Prepare: the coordinator generates a ballot (Ballot in our case) and asks replicas to
     *     - promise not to accept updates from older ballots and
     *     - tell us about the latest ballots it has already _promised_, _accepted_, or _committed_
     *     - reads the necessary data to evaluate our CAS condition
     *
     *  2. Propose: if a majority of replicas reply, the coordinator asks replicas to accept the value of the
     *     highest proposal ballot it heard about, or a new value if no in-progress proposals were reported.
     *  3. Commit (Learn): if a majority of replicas acknowledge the accept request, we can commit the new
     *     value.
     *
     *  Commit procedure is not covered in "Paxos Made Simple," and only briefly mentioned in "Paxos Made Live,"
     *  so here is our approach:
     *   3a. The coordinator sends a commit message to all replicas with the ballot and value.
     *   3b. Because of 1-2, this will be the highest-seen commit ballot.  The replicas will note that,
     *       and send it with subsequent promise replies.  This allows us to discard acceptance records
     *       for successfully committed replicas, without allowing incomplete proposals to commit erroneously
     *       later on.
     *
     *  Note that since we are performing a CAS rather than a simple update, when nodes respond positively to
     *  Prepare, they include read response of commited values that will be reconciled on the coordinator
     *  and checked against CAS precondition between the prepare and accept phases. This gives us a slightly
     *  longer window for another coordinator to come along and trump our own promise with a newer one but
     *  is otherwise safe.
     *
     *  Any successful prepare phase yielding a read that rejects the condition must be followed by the proposal of
     *  an empty update, to ensure the evaluation of the condition is linearized with respect to other reads and writes.
     *
     * @param partitionKey the row key for the row to CAS
     * @param request the conditions for the CAS to apply as well as the update to perform if the conditions hold.
     * @param consistencyForConsensus the consistency for the paxos prepare and propose round. This can only be either SERIAL or LOCAL_SERIAL.
     * @param consistencyForCommit the consistency for write done during the commit phase. This can be anything, except SERIAL or LOCAL_SERIAL.
     *
     * @return null if the operation succeeds in updating the row, or the current values corresponding to conditions.
     * (since, if the CAS doesn't succeed, it means the current value do not match the conditions).
     */
    public static ConsensusAttemptResult cas(DecoratedKey partitionKey,
                                             CASRequest request,
                                             ConsistencyLevel consistencyForConsensus,
                                             ConsistencyLevel consistencyForCommit,
                                             ClientState clientState,
                                             Dispatcher.RequestTime requestTime)
            throws UnavailableException, IsBootstrappingException, RequestFailureException, RequestTimeoutException, InvalidRequestException
    {
        final long proposeDeadline = requestTime.startedAtNanos() + getCasContentionTimeout(NANOSECONDS);
        final long commitDeadline = Math.max(proposeDeadline, requestTime.startedAtNanos() + getWriteRpcTimeout(NANOSECONDS));
        SinglePartitionReadCommand readCommand = request.readCommand(FBUtilities.nowInSeconds());
        TableMetadata metadata = readCommand.metadata();

        consistencyForConsensus.validateForCas();
        consistencyForCommit.validateForCasCommit(Keyspace.open(metadata.keyspace).getReplicationStrategy());

        int failedAttemptsDueToContention = 0;
        try (PaxosOperationLock lock = PaxosState.lock(partitionKey, metadata, proposeDeadline, consistencyForConsensus, true))
        {
            Paxos.Async<PaxosCommit.Status> commit = null;
            done: while (true)
            {
                // read the current values and check they validate the conditions
                Tracing.trace("Reading existing values for CAS precondition");

                BeginResult begin = begin(proposeDeadline, readCommand, consistencyForConsensus,
                        true, null, failedAttemptsDueToContention);

                if (begin.retryWithNewConsenusProtocol)
                {
                    casWriteMetrics.beginMigrationRejects.mark();
                    return RETRY_NEW_PROTOCOL;
                }

                Ballot ballot = begin.ballot;
                Participants participants = begin.participants;
                failedAttemptsDueToContention = begin.failedAttemptsDueToContention;

                FilteredPartition current;
                try (RowIterator iter = PartitionIterators.getOnlyElement(begin.readResponse, readCommand))
                {
                    current = FilteredPartition.create(iter);
                }

                Proposal proposal;
                boolean conditionMet = request.appliesTo(current);
                if (!conditionMet)
                {
                    if (getPaxosVariant() == v2_without_linearizable_reads_or_rejected_writes)
                    {
                        Tracing.trace("CAS precondition rejected", current);
                        casWriteMetrics.conditionNotMet.inc();
                        return casResult(current.rowIterator(false));
                    }

                    // If we failed to meet our condition, it does not mean we can do nothing: if we do not propose
                    // anything that is accepted by a quorum, it is possible for our !conditionMet state
                    // to not be serialized wrt other operations.
                    // If a later read encounters an "in progress" write that did not reach a majority,
                    // but that would have permitted conditionMet had it done so (and hence we evidently did not witness),
                    // that operation will complete the in-progress proposal before continuing, so that this and future
                    // reads will perceive conditionMet without any intervening modification from the time at which we
                    // assured a conditional write that !conditionMet.
                    // So our evaluation is only serialized if we invalidate any in progress operations by proposing an empty update
                    // See also CASSANDRA-12126
                    if (begin.isLinearizableRead)
                    {
                        Tracing.trace("CAS precondition does not match current values {}; read is already linearizable; aborting", current);
                        return casResult(conditionNotMet(current));
                    }

                    Tracing.trace("CAS precondition does not match current values {}; proposing empty update", current);
                    proposal = Proposal.empty(ballot, partitionKey, metadata);
                }
                else if (begin.isPromised)
                {
                    // finish the paxos round w/ the desired updates
                    // TODO "turn null updates into delete?" - what does this TODO even mean?
                    PartitionUpdate updates = request.makeUpdates(current, clientState, begin.ballot);

                    // Update the metrics before triggers potentially add mutations.
                    ClientRequestSizeMetrics.recordRowAndColumnCountMetrics(updates);

                    // Apply triggers to cas updates. A consideration here is that
                    // triggers emit Mutations, and so a given trigger implementation
                    // may generate mutations for partitions other than the one this
                    // paxos round is scoped for. In this case, TriggerExecutor will
                    // validate that the generated mutations are targetted at the same
                    // partition as the initial updates and reject (via an
                    // InvalidRequestException) any which aren't.
                    updates = TriggerExecutor.instance.execute(updates);

                    proposal = Proposal.of(ballot, updates);
                    Tracing.trace("CAS precondition is met; proposing client-requested updates for {}", ballot);
                }
                else
                {
                    // must retry, as only achieved read success in begin
                    Tracing.trace("CAS precondition is met, but ballot stale for proposal; retrying", current);
                    continue;
                }

                PaxosPropose.Status propose = propose(proposal, participants, conditionMet, false).awaitUntil(proposeDeadline);
                switch (propose.outcome)
                {
                    default: throw new IllegalStateException();

                    case MAYBE_FAILURE:
                        throw propose.maybeFailure().markAndThrowAsTimeoutOrFailure(true, consistencyForConsensus, failedAttemptsDueToContention);

                    case SUCCESS:
                    {
                        if (!conditionMet)
                            return casResult(conditionNotMet(current));

                        // no need to commit a no-op; either it
                        //   1) reached a majority, in which case it was agreed, had no effect and we can do nothing; or
                        //   2) did not reach a majority, was not agreed, and was not user visible as a result so we can ignore it
                        if (!proposal.update.isEmpty())
                            commit = commit(proposal.agreed(), participants, consistencyForConsensus, consistencyForCommit, true);

                        break done;
                    }

                    case SUPERSEDED:
                    {
                        Superseded superseded = propose.superseded();
                        switch (superseded.hadSideEffects)
                        {
                            default: throw new IllegalStateException();

                            case MAYBE:
                                // We don't know if our update has been applied, as the competing ballot may have completed
                                // our proposal.  We yield our uncertainty to the caller via timeout exception.
                                // TODO: should return more useful result to client, and should also avoid this situation where possible
                                throw new MaybeFailure(false, participants.sizeOfPoll(), participants.sizeOfConsensusQuorum, 0, emptyMap())
                                        .markAndThrowAsTimeoutOrFailure(true, consistencyForConsensus, failedAttemptsDueToContention);

                            case NO:
                                // Shouldn't retry on this protocol
                                if (superseded.needsConsensusMigration)
                                {
                                    casWriteMetrics.acceptMigrationRejects.mark();
                                    return RETRY_NEW_PROTOCOL;
                                }
                                // We have been superseded without our proposal being accepted by anyone, so we can safely retry
                                Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
                                if (!waitForContention(proposeDeadline, ++failedAttemptsDueToContention, metadata, partitionKey, consistencyForConsensus, WRITE))
                                    throw MaybeFailure.noResponses(participants).markAndThrowAsTimeoutOrFailure(true, consistencyForConsensus, failedAttemptsDueToContention);
                        }
                    }
                }
                // continue to retry
            }

            if (commit != null)
            {
                PaxosCommit.Status result = commit.awaitUntil(commitDeadline);
                if (!result.isSuccess())
                    throw result.maybeFailure().markAndThrowAsTimeoutOrFailure(true, consistencyForCommit, failedAttemptsDueToContention);
            }
            Tracing.trace("CAS successful");
            return casResult((RowIterator)null);

        }
        finally
        {
            final long latency = nanoTime() - requestTime.startedAtNanos();

            if (failedAttemptsDueToContention > 0)
            {
                casWriteMetrics.contention.update(failedAttemptsDueToContention);
                openAndGetStore(metadata).metric.topCasPartitionContention.addSample(partitionKey.getKey(), failedAttemptsDueToContention);
            }


            casWriteMetrics.addNano(latency);
            writeMetricsMap.get(consistencyForConsensus).addNano(latency);
        }
    }

    private static RowIterator conditionNotMet(FilteredPartition read)
    {
        Tracing.trace("CAS precondition rejected", read);
        casWriteMetrics.conditionNotMet.inc();
        return read.rowIterator(false);
    }

    public static ConsensusAttemptResult read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyForConsensus, Dispatcher.RequestTime requestTime)
            throws InvalidRequestException, UnavailableException, ReadFailureException, ReadTimeoutException
    {
        long start = nanoTime();
        if (group.queries.size() > 1)
            throw new InvalidRequestException("SERIAL/LOCAL_SERIAL consistency may only be requested for one partition at a time");
        long deadline = requestTime.computeDeadline(DatabaseDescriptor.getReadRpcTimeout(NANOSECONDS));

        int failedAttemptsDueToContention = 0;
        Ballot minimumBallot = null;
        SinglePartitionReadCommand read = group.queries.get(0);
        try (PaxosOperationLock lock = PaxosState.lock(read.partitionKey(), read.metadata(), deadline, consistencyForConsensus, false))
        {
            while (true)
            {
                // does the work of applying in-progress writes; throws UAE or timeout if it can't
                final BeginResult begin = begin(deadline, read, consistencyForConsensus, false, minimumBallot, failedAttemptsDueToContention);
                failedAttemptsDueToContention = begin.failedAttemptsDueToContention;

                if (begin.retryWithNewConsenusProtocol)
                {
                    casReadMetrics.beginMigrationRejects.mark();
                    return RETRY_NEW_PROTOCOL;
                }

                switch (PAXOS_VARIANT)
                {
                    default:
                        if (!read.metadata().keyspace.equals(SchemaConstants.METADATA_KEYSPACE_NAME))
                            throw new AssertionError();

                    case v2_without_linearizable_reads_or_rejected_writes:
                    case v2_without_linearizable_reads:
                        return serialReadResult(begin.readResponse);

                    case v2:
                        // no need to submit an empty proposal, as the promise will be treated as complete for future optimistic reads
                        if (begin.isLinearizableRead)
                            return serialReadResult(begin.readResponse);
                }

                Proposal proposal = Proposal.empty(begin.ballot, read.partitionKey(), read.metadata());
                PaxosPropose.Status propose = propose(proposal, begin.participants, true, false).awaitUntil(deadline);
                switch (propose.outcome)
                {
                    default: throw new IllegalStateException();

                    case MAYBE_FAILURE:
                        throw propose.maybeFailure().markAndThrowAsTimeoutOrFailure(false, consistencyForConsensus, failedAttemptsDueToContention);

                    case SUCCESS:
                        return serialReadResult(begin.readResponse);

                    case SUPERSEDED:
                        Superseded superseded = propose.superseded();
                        // For consensus migration we are going to bail out earlier if migration is needed
                        // otherwise it it will fail every single query that races with migration being started
                        // during the propose step. Necessary because of CASSANDRA-18276
                        // Shouldn't retry again on this protocol
                        if (superseded.needsConsensusMigration)
                        {
                            casReadMetrics.acceptMigrationRejects.mark();
                            return RETRY_NEW_PROTOCOL;
                        }
                        // TODO https://issues.apache.org/jira/browse/CASSANDRA-18276 side effects shouldn't matter for reads
                        switch (superseded.hadSideEffects)
                        {
                            default: throw new IllegalStateException();

                            case MAYBE:
                                // We don't know if our update has been applied, as the competing ballot may have completed
                                // our proposal.  We yield our uncertainty to the caller via timeout exception.
                                // TODO: should return more useful result to client, and should also avoid this situation where possible
                                throw new MaybeFailure(false, begin.participants.sizeOfPoll(), begin.participants.sizeOfConsensusQuorum, 0, emptyMap())
                                      .markAndThrowAsTimeoutOrFailure(true, consistencyForConsensus, failedAttemptsDueToContention);

                            case NO:
                                minimumBallot = propose.superseded().by;
                                // We have been superseded without our proposal being accepted by anyone, so we can safely retry
                                Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
                                if (!waitForContention(deadline, ++failedAttemptsDueToContention, group.metadata(), group.queries.get(0).partitionKey(), consistencyForConsensus, READ))
                                    throw MaybeFailure.noResponses(begin.participants).markAndThrowAsTimeoutOrFailure(true, consistencyForConsensus, failedAttemptsDueToContention);
                        }
                        break;
                }
            }
        }
        finally
        {
            // We don't base latency tracking on the startedAtNanos of the RequestTime because queries which involve
            // internal paging may be composed of multiple distinct reads, whereas RequestTime relates to the single
            // client request. This is a measure of how long this specific individual read took, not total time since
            // processing of the client began.
            long latency = nanoTime() - start;
            readMetrics.addNano(latency);
            casReadMetrics.addNano(latency);
            readMetricsMap.get(consistencyForConsensus).addNano(latency);
            TableMetadata table = read.metadata();
            Keyspace.open(table.keyspace).getColumnFamilyStore(table.name).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);
            if (failedAttemptsDueToContention > 0)
                casReadMetrics.contention.update(failedAttemptsDueToContention);
        }
    }

    static class BeginResult
    {
        final Ballot ballot;
        final Participants participants;
        final int failedAttemptsDueToContention;
        final PartitionIterator readResponse;
        final boolean isLinearizableRead;
        final boolean isPromised;
        final Ballot retryWithAtLeast;

        final boolean retryWithNewConsenusProtocol;

        public BeginResult(Ballot ballot, Participants participants, int failedAttemptsDueToContention, PartitionIterator readResponse, boolean isLinearizableRead, boolean isPromised, Ballot retryWithAtLeast, boolean retryWithNewConsenusProtocol)
        {
            assert isPromised || isLinearizableRead || retryWithNewConsenusProtocol;
            this.ballot = ballot;
            this.participants = participants;
            this.failedAttemptsDueToContention = failedAttemptsDueToContention;
            this.readResponse = readResponse;
            this.isLinearizableRead = isLinearizableRead;
            this.isPromised = isPromised;
            this.retryWithAtLeast = retryWithAtLeast;
            this.retryWithNewConsenusProtocol = retryWithNewConsenusProtocol;
        }

        static BeginResult retryOnNewProtocol()
        {
            return new BeginResult(null, null, -1, null, false, false, null, true);
        }
    }

    /**
     * Begin a Paxos operation by seeking promises from our electorate to be completed with proposals by our caller; and:
     *
     *  - Completing any in-progress proposals witnessed, that are not known to have reached the commit phase
     *  - Completing any in-progress commits witnessed, that are not known to have reached a quorum of the electorate
     *  - Retrying and backing-off under contention
     *  - Detecting electorate mismatches with our peers and retrying to avoid non-overlapping
     *    electorates agreeing operations
     *  - Returning a resolved read response, and knowledge of if it is linearizable to read without proposing an empty update
     *
     * Optimisations:
     *    - If the promises report an incomplete commit (but have been able to witness it in a read response)
     *      we will submit the commit to those nodes that have not witnessed while waiting for those that have,
     *      returning as soon as a quorum is known to have witnessed the commit
     *    - If we witness an in-progress commit to complete, we batch the commit together with a new prepare
     *      restarting our operation.
     *    - If we witness an in-progress proposal to complete, after successfully proposing it we batch its
     *      commit together with a new prepare restarting our operation.
     *
     * @return the Paxos ballot promised by the replicas if no in-progress requests were seen and a quorum of
     * nodes have seen the mostRecentCommit.  Otherwise, return null.
     */
    private static BeginResult begin(long deadline,
                                     SinglePartitionReadCommand query,
                                     ConsistencyLevel consistencyForConsensus,
                                     final boolean isWrite,
                                     Ballot minimumBallot,
                                     int failedAttemptsDueToContention)
            throws WriteTimeoutException, WriteFailureException, ReadTimeoutException, ReadFailureException
    {
        boolean acceptEarlyReadPermission = !isWrite; // if we're reading, begin by assuming a read permission is sufficient
        Participants initialParticipants = Participants.get(query.metadata(), query.partitionKey(), consistencyForConsensus);
        initialParticipants.assureSufficientLiveNodes(isWrite);
        PaxosPrepare preparing = prepare(minimumBallot, initialParticipants, query, isWrite, acceptEarlyReadPermission);
        while (true)
        {
            // prepare
            PaxosPrepare retry = null;
            PaxosPrepare.Status prepare = preparing.awaitUntil(deadline);

            // After performing the prepare phase we may discover that we can't propose
            // our own transaction on this protocol by discovering a new CM Epoch
            if (ConsensusRequestRouter.instance.isKeyInMigratingOrMigratedRangeDuringPaxosBegin(query.metadata().id, query.partitionKey()) && prepare.outcome != MAYBE_FAILURE)
            {
                return BeginResult.retryOnNewProtocol();
            }

            boolean isPromised = false;
            retry: switch (prepare.outcome)
            {
                default: throw new IllegalStateException();

                case FOUND_INCOMPLETE_COMMITTED:
                {
                    FoundIncompleteCommitted incomplete = prepare.incompleteCommitted();
                    Tracing.trace("Repairing replicas that missed the most recent commit");
                    retry = commitAndPrepare(incomplete.committed, incomplete.participants, query, isWrite, acceptEarlyReadPermission);
                    break;
                }
                case FOUND_INCOMPLETE_ACCEPTED:
                {
                    FoundIncompleteAccepted inProgress = prepare.incompleteAccepted();
                    Tracing.trace("Finishing incomplete paxos round {}", inProgress.accepted);
                    if (isWrite)
                        casWriteMetrics.unfinishedCommit.inc();
                    else
                        casReadMetrics.unfinishedCommit.inc();

                    // we DO NOT need to change the timestamp of this commit - either we or somebody else will finish it
                    // and the original timestamp is correctly linearised. By not updatinig the timestamp we leave enough
                    // information for nodes to avoid competing re-proposing the same proposal; if an in progress accept
                    // is equal to the latest commit (even if the ballots aren't) we're done and can abort earlier,
                    // and in fact it's possible for a CAS to sometimes determine if side effects occurred by reading
                    // the underlying data and not witnessing the timestamp of its ballot (or any newer for the relevant data).
                    Proposal repropose = new Proposal(inProgress.ballot, inProgress.accepted.update);
                    PaxosPropose.Status proposeResult = propose(repropose, inProgress.participants, false, true).awaitUntil(deadline);
                    switch (proposeResult.outcome)
                    {
                        default: throw new IllegalStateException();

                        case MAYBE_FAILURE:
                            throw proposeResult.maybeFailure().markAndThrowAsTimeoutOrFailure(isWrite, consistencyForConsensus, failedAttemptsDueToContention);

                        case SUCCESS:
                            retry = commitAndPrepare(repropose.agreed(), inProgress.participants, query, isWrite, acceptEarlyReadPermission);
                            break retry;

                        case SUPERSEDED:
                            checkState(!proposeResult.superseded().needsConsensusMigration, "Should not receive needsConsensusMigration rejects from begin");
                            // since we are proposing a previous value that was maybe superseded by us before completion
                            // we don't need to test the side effects, as we just want to start again, and fall through
                            // to the superseded section below
                            prepare = new PaxosPrepare.Superseded(proposeResult.superseded().by, inProgress.participants);
                    }
                }

                case SUPERSEDED:
                {
                    Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                    // sleep a random amount to give the other proposer a chance to finish
                    if (!waitForContention(deadline, ++failedAttemptsDueToContention, query.metadata(), query.partitionKey(), consistencyForConsensus, isWrite ? WRITE : READ))
                        throw MaybeFailure.noResponses(prepare.participants).markAndThrowAsTimeoutOrFailure(true, consistencyForConsensus, failedAttemptsDueToContention);
                    retry = prepare(prepare.retryWithAtLeast(), prepare.participants, query, isWrite, acceptEarlyReadPermission);
                    break;
                }
                case PROMISED: isPromised = true;
                case READ_PERMITTED:
                {
                    // We have received a quorum of promises (or read permissions) that have all witnessed the commit of the prior paxos
                    // round's proposal (if any).
                    PaxosPrepare.Success success = prepare.success();

                    Supplier<Participants> plan = () -> success.participants;
                    DataResolver<?, ?> resolver = new DataResolver<>(ReadCoordinator.DEFAULT, query, plan, NoopReadRepair.instance, new Dispatcher.RequestTime(query.creationTimeNanos()));
                    for (int i = 0 ; i < success.responses.size() ; ++i)
                        resolver.preprocess(success.responses.get(i));

                    class WasRun implements Runnable { boolean v; public void run() { v = true; } }
                    WasRun hadShortRead = new WasRun();
                    PartitionIterator result = resolver.resolve(hadShortRead);

                    if (!isPromised && hadShortRead.v)
                    {
                        // we need to propose an empty update to linearize our short read, but only had read success
                        // since we may continue to perform short reads, we ask our prepare not to accept an early
                        // read permission, when a promise may yet be obtained
                        // TODO: increase read size each time this happens?
                        acceptEarlyReadPermission = false;
                        break;
                    }

                    return new BeginResult(success.ballot, success.participants, failedAttemptsDueToContention, result, !hadShortRead.v && success.isReadSafe, isPromised, success.supersededBy, false);
                }

                case MAYBE_FAILURE:
                    throw prepare.maybeFailure().markAndThrowAsTimeoutOrFailure(isWrite, consistencyForConsensus, failedAttemptsDueToContention);

                case ELECTORATE_MISMATCH:
                    Participants participants = Participants.get(query.metadata(), query.partitionKey(), consistencyForConsensus);
                    participants.assureSufficientLiveNodes(isWrite);
                    retry = prepare(participants, query, isWrite, acceptEarlyReadPermission);
                    break;

            }

            if (retry == null)
            {
                Tracing.trace("Some replicas have already promised a higher ballot than ours; retrying");
                // sleep a random amount to give the other proposer a chance to finish
                if (!waitForContention(deadline, ++failedAttemptsDueToContention, query.metadata(), query.partitionKey(), consistencyForConsensus, isWrite ? WRITE : READ))
                    throw MaybeFailure.noResponses(prepare.participants).markAndThrowAsTimeoutOrFailure(true, consistencyForConsensus, failedAttemptsDueToContention);
                retry = prepare(prepare.retryWithAtLeast(), prepare.participants, query, isWrite, acceptEarlyReadPermission);
            }

            preparing = retry;
        }
    }

    public static boolean isInRangeAndShouldProcess(InetAddressAndPort from, DecoratedKey key, TableMetadata table, boolean includesRead)
    {
        Keyspace keyspace = Keyspace.open(table.keyspace);
        // MetaStrategy distributes the entire keyspace to all replicas. In addition, its tables (currently only
        // the dist log table) don't use the globally configured partitioner. For these reasons we don't lookup the
        // replicas using the supplied token as this can actually be of the incorrect type (for example when
        // performing Paxos repair).
        Token token = table.partitioner == MetaStrategy.partitioner ? MetaStrategy.entireRange.right : key.getToken();
        return (includesRead ? EndpointsForToken.natural(keyspace, token).get()
                             : ReplicaLayout.forTokenWriteLiveAndDown(keyspace, token).all()
        ).contains(getBroadcastAddressAndPort());
    }

    static ConsistencyLevel nonSerial(ConsistencyLevel serial)
    {
        switch (serial)
        {
            default: throw new IllegalStateException();
            case SERIAL: return QUORUM;
            case LOCAL_SERIAL: return LOCAL_QUORUM;
        }
    }

    private static void mark(boolean isWrite, Function<ClientRequestMetrics, Meter> toMark, ConsistencyLevel consistency)
    {
        if (isWrite)
        {
            toMark.apply(casWriteMetrics).mark();
            toMark.apply(writeMetricsMap.get(consistency)).mark();
        }
        else
        {
            toMark.apply(casReadMetrics).mark();
            toMark.apply(readMetricsMap.get(consistency)).mark();
        }
    }

    public static Ballot newBallot(@Nullable Ballot minimumBallot, ConsistencyLevel consistency)
    {
        // We want a timestamp that is guaranteed to be unique for that node (so that the ballot is globally unique), but if we've got a prepare rejected
        // already we also want to make sure we pick a timestamp that has a chance to be promised, i.e. one that is greater that the most recently known
        // in progress (#5667). Lastly, we don't want to use a timestamp that is older than the last one assigned by ClientState or operations may appear
        // out-of-order (#7801).
        long minTimestampMicros = minimumBallot == null ? Long.MIN_VALUE : 1 + minimumBallot.unixMicros();
        // Note that ballotMicros is not guaranteed to be unique if two proposal are being handled concurrently by the same coordinator. But we still
        // need ballots to be unique for each proposal so we have to use getRandomTimeUUIDFromMicros.
        return nextBallot(minTimestampMicros, flag(consistency));
    }

    static Ballot staleBallotNewerThan(Ballot than, ConsistencyLevel consistency)
    {
        long minTimestampMicros = 1 + than.unixMicros();
        long maxTimestampMicros = BallotGenerator.Global.prevUnixMicros();
        maxTimestampMicros -= Math.min((maxTimestampMicros - minTimestampMicros) / 2, SECONDS.toMicros(5L));
        if (maxTimestampMicros <= minTimestampMicros)
            return nextBallot(minTimestampMicros, flag(consistency));

        return staleBallot(minTimestampMicros, maxTimestampMicros, flag(consistency));
    }

    /**
     * Create a ballot uuid with the consistency level encoded in the timestamp.
     *
     * UUIDGen.getRandomTimeUUIDFromMicros timestamps are always a multiple of 10, so we add a 1 or 2 to indicate
     * the consistency level of the operation. This should have no effect in practice (except preferring a serial
     * operation over a local serial if there's a timestamp collision), but lets us avoid adding CL to the paxos
     * table and messages, which should make backcompat easier if a different solution is committed upstream.
     */
    public static Ballot ballotForConsistency(long whenInMicros, ConsistencyLevel consistency)
    {
        Preconditions.checkArgument(consistency.isSerialConsistency());
        return nextBallot(whenInMicros, flag(consistency));
    }

    private static Ballot.Flag flag(ConsistencyLevel consistency)
    {
        return consistency == SERIAL ? GLOBAL : LOCAL;
    }

    public static ConsistencyLevel consistency(Ballot ballot)
    {
        switch (ballot.flag())
        {
            default: return null;
            case LOCAL: return LOCAL_SERIAL;
            case GLOBAL: return SERIAL;
        }
    }

    static Map<InetAddressAndPort, EndpointState> verifyElectorate(Electorate remoteElectorate, Electorate localElectorate)
    {
        // verify electorates; if they differ, send back gossip info for superset of two participant sets
        if (remoteElectorate.equals(localElectorate))
            return emptyMap();

        Map<InetAddressAndPort, EndpointState> endpoints = Maps.newHashMapWithExpectedSize(remoteElectorate.size() + localElectorate.size());
        for (InetAddressAndPort host : remoteElectorate)
        {
            EndpointState endpoint = Gossiper.instance.copyEndpointStateForEndpoint(host);
            if (endpoint == null)
            {
                NoSpamLogger.log(logger, WARN, 1, TimeUnit.MINUTES, "Remote electorate {} could not be found in Gossip", host);
                continue;
            }
            endpoints.put(host, endpoint);
        }
        for (InetAddressAndPort host : localElectorate)
        {
            EndpointState endpoint = Gossiper.instance.copyEndpointStateForEndpoint(host);
            if (endpoint == null)
            {
                NoSpamLogger.log(logger, WARN, 1, TimeUnit.MINUTES, "Local electorate {} could not be found in Gossip", host);
                continue;
            }
            endpoints.putIfAbsent(host, endpoint);
        }

        return endpoints;
    }

    public static boolean useV2()
    {
        switch (PAXOS_VARIANT)
        {
            case v2_without_linearizable_reads_or_rejected_writes:
            case v2_without_linearizable_reads:
            case v2:
                return true;
            case v1:
            case v1_without_linearizable_reads_or_rejected_writes:
                return false;
            default:
                throw new AssertionError();
        }
    }

    public static boolean isLinearizable()
    {
        switch (PAXOS_VARIANT)
        {
            case v2:
            case v1:
                return true;
            case v2_without_linearizable_reads_or_rejected_writes:
            case v2_without_linearizable_reads:
            case v1_without_linearizable_reads_or_rejected_writes:
                return false;
            default:
                throw new AssertionError();
        }
    }

    public static void setPaxosVariant(Config.PaxosVariant paxosVariant)
    {
        Preconditions.checkNotNull(paxosVariant);
        PAXOS_VARIANT = paxosVariant;
        DatabaseDescriptor.setPaxosVariant(paxosVariant);
    }

    public static Config.PaxosVariant getPaxosVariant()
    {
        return PAXOS_VARIANT;
    }

    static boolean isOldParticipant(Replica replica)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId addr = metadata.directory.peerId(replica.endpoint());
        CassandraVersion version = ClusterMetadata.current().directory.version(addr).cassandraVersion;
        if (version == null)
            return false;

        try
        {
            return version.compareTo(MODERN_PAXOS_RELEASE) < 0;
        }
        catch (Throwable t)
        {
            return false;
        }
    }

    public static void evictHungRepairs()
    {
        PaxosRepairState.instance().evictHungRepairs();
    }
}
