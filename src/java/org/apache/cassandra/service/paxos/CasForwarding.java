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

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.replication.migration.MigrationRouter;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.Verb.CONSENSUS_READ_FORWARD_REQ;

public class CasForwarding
{
    private static final Logger logger = LoggerFactory.getLogger(CasForwarding.class);
    private static final boolean DISABLE_CONSENSUS_REQUEST_FORWARDING = CassandraRelevantProperties.DISABLE_CONSENSUS_REQUEST_FORWARDING.getBoolean();

    /**
     * Outcome of a forwarding check, distinguishing "no forwarding was needed" from "the request was
     * forwarded, and this was its result".
     */
    public static final class Forwarded<T>
    {
        public final T result;

        public Forwarded(T result)
        {
            this.result = result;
        }
    }

    public static RuntimeException casForwardingFailure(Throwable t, ConsistencyLevel consistencyForPaxos, int blockFor)
    {
        MessagingService.FailureResponseException failure = forwardingFailure(t);
        if (failure == null)
            return new RuntimeException("Failed to forward CAS operation to replica coordinator", t);

        if (failure.failureReason() == RequestFailureReason.TIMEOUT)
            return new CasWriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, blockFor, 0);

        return new WriteFailureException(consistencyForPaxos, 0, blockFor, WriteType.CAS,
                                         ImmutableMap.of(failure.from(), failure.failureReason()));
    }

    public static RuntimeException readForwardingFailure(Throwable t, ConsistencyLevel consistencyLevel, int blockFor)
    {
        MessagingService.FailureResponseException failure = forwardingFailure(t);
        if (failure == null)
            return new RuntimeException("Failed to forward consensus read operation to replica coordinator", t);

        if (failure.failureReason() == RequestFailureReason.TIMEOUT)
            return new ReadTimeoutException(consistencyLevel, 0, blockFor, false);

        return new ReadFailureException(consistencyLevel, 0, blockFor, false,
                                        ImmutableMap.of(failure.from(), failure.failureReason()));
    }

    public static MessagingService.FailureResponseException forwardingFailure(Throwable t)
    {
        for (Throwable cause = t; cause != null; cause = cause.getCause())
        {
            if (cause instanceof MessagingService.FailureResponseException)
                return (MessagingService.FailureResponseException) cause;
        }
        return null;
    }

    /**
     * Check if a CAS operation needs to be forwarded to a replica coordinator for tracked keyspaces.
     * Returns null if no forwarding is needed, otherwise the forwarded operation's result wrapped in a
     * {@link CasForwarding.Forwarded} — which may hold a null result, since a CAS that applied reports none.
     */
    public static CasForwarding.Forwarded<RowIterator> checkAndForwardCasIfNeeded(String keyspaceName,
                                                                                  String cfName,
                                                                                  DecoratedKey key,
                                                                                  CQL3CasRequest request,
                                                                                  ConsistencyLevel consistencyForPaxos,
                                                                                  ConsistencyLevel consistencyForCommit,
                                                                                  ClientState clientState,
                                                                                  long nowInSeconds,
                                                                                  boolean alreadyForwarded)
    throws UnavailableException, RequestFailureException, RequestTimeoutException
    {
        Keyspace keyspace = Keyspace.openIfExists(keyspaceName);
        if (keyspace == null)
            throw new KeyspaceNotDefinedException("Keyspace " + keyspaceName + " does not exist");

        ClusterMetadata cm = ClusterMetadata.current();
        TableMetadata tableMetadata = cm.schema.getTableMetadata(keyspaceName, cfName);
        if (tableMetadata == null || !MigrationRouter.shouldUseTrackedForWrites(cm, keyspaceName, tableMetadata.id, key.getToken()))
            return null; // Not tracked, no forwarding needed

        // Property to disable top-level forwarding for testing
        if (DISABLE_CONSENSUS_REQUEST_FORWARDING)
            return null;

        // Check if current coordinator is not a replica
        Token tk = key.getToken();
        EndpointsForToken allReplicas = ReplicaLayout.forTokenWriteLiveAndDown(cm, keyspace, tk)
                                                     .all();
        EndpointsForToken liveReplicas = allReplicas.filter(FailureDetector.isReplicaAlive);

        InetAddressAndPort localEndpoint = FBUtilities.getBroadcastAddressAndPort();
        boolean isLocalReplica = allReplicas.contains(localEndpoint);

        if (isLocalReplica)
            return null; // Local node is a replica, no forwarding needed

        // If this request was already forwarded to us and we're not a replica, something is wrong
        if (alreadyForwarded)
        {
            logger.error("Received forwarded CAS for keyspace {} table {} key {} but local node {} is not a replica. Replicas are: {}",
                         keyspaceName, cfName, key, localEndpoint, allReplicas);
            Tracing.trace("ERROR: Received forwarded CAS but local node is not a replica");
            throw new InvalidRequestException("Forwarded CAS received by non-replica node " + localEndpoint);
        }

        // Find best replica to forward to using proximity-based selection
        if (liveReplicas.isEmpty())
            throw new UnavailableException("No live replicas available for CAS forwarding", consistencyForPaxos, 1, 0);

        // Sort by proximity and select the best coordinator
        EndpointsForToken sortedReplicas = DatabaseDescriptor.getNodeProximity().sortedByProximity(localEndpoint, liveReplicas);
        InetAddressAndPort replicaCoordinator = sortedReplicas.get(0).endpoint();

        // Create forward request
        CasForwardRequest forwardRequest =
        new CasForwardRequest(keyspaceName, cfName, key, consistencyForPaxos, consistencyForCommit,
                              nowInSeconds, clientState, request);
        Message<CasForwardRequest> message = Message.out(Verb.CAS_FORWARD_REQ, forwardRequest);

        try
        {
            // Send synchronous request to replica coordinator
            Object responseObj = MessagingService.instance().sendWithResult(message, replicaCoordinator).get();
            @SuppressWarnings("unchecked")
            Message<CasForwardResponse> responseMessage = (Message<CasForwardResponse>) responseObj;
            CasForwardResponse response = responseMessage.payload;

            // Add warnings from forwarded operation to local ClientWarn
            for (String warning : response.warnings)
                ClientWarn.instance.warn(warning);

            // Check if the forwarded operation had an exception
            if (!response.isSuccess())
            {
                throw response.exception;
            }

            // Wrap even when the result is absent: a CAS that applied reports no result, and an
            // unwrapped null would be indistinguishable from "no forwarding was needed".
            return new CasForwarding.Forwarded<>(response.rowIterator());
        }
        catch (CassandraException ce)
        {
            // Rethrow CassandraExceptions from the replica coordinator
            throw ce;
        }
        catch (Exception e)
        {
            throw CasForwarding.casForwardingFailure(e, consistencyForPaxos,
                                                     consistencyForPaxos.blockFor(keyspace.getReplicationStrategy()));
        }
    }

    /**
     * Check if a consensus read operation needs to be forwarded to a replica coordinator for tracked keyspaces.
     * Returns null if no forwarding is needed, otherwise the forwarded read's result wrapped in a
     * {@link CasForwarding.Forwarded}.
     */
    public static CasForwarding.Forwarded<PartitionIterator> checkAndForwardConsensusReadIfNeeded(SinglePartitionReadCommand.Group group,
                                                                                                  ConsistencyLevel consistencyLevel,
                                                                                                  boolean alreadyForwarded)
    throws UnavailableException, ReadFailureException, ReadTimeoutException
    {
        if (group.queries.isEmpty())
            return null;

        // Use the first command to determine keyspace and key for replica planning
        SinglePartitionReadCommand firstCommand = group.queries.get(0);
        String keyspaceName = firstCommand.metadata().keyspace;

        Keyspace keyspace = Keyspace.openIfExists(keyspaceName);
        if (keyspace == null)
            throw new KeyspaceNotDefinedException("Keyspace " + keyspaceName + " does not exist");

        ClusterMetadata cm = ClusterMetadata.current();
        if (!MigrationRouter.shouldUseTracked(cm, firstCommand))
            return null; // Not tracked, no forwarding needed

        // Property to disable top-level forwarding for testing
        if (DISABLE_CONSENSUS_REQUEST_FORWARDING)
            return null;

        // Check if current coordinator is not a replica
        Token tk = firstCommand.partitionKey().getToken();
        EndpointsForToken allReplicas = ReplicaLayout.forTokenWriteLiveAndDown(cm, keyspace, tk)
                                                     .all();
        EndpointsForToken liveReplicas = allReplicas.filter(FailureDetector.isReplicaAlive);

        InetAddressAndPort localEndpoint = FBUtilities.getBroadcastAddressAndPort();
        boolean isLocalReplica = allReplicas.contains(localEndpoint);

        if (isLocalReplica)
            return null; // Local node is a replica, no forwarding needed

        // If this request was already forwarded to us and we're not a replica, something is wrong
        if (alreadyForwarded)
        {
            logger.error("Received forwarded consensus read for keyspace {} key {} but local node {} is not a replica. Replicas are: {}",
                         keyspaceName, firstCommand.partitionKey(), localEndpoint, allReplicas);
            Tracing.trace("ERROR: Received forwarded consensus read but local node is not a replica");
            throw new RuntimeException("Forwarded consensus read received by non-replica node " + localEndpoint);
        }

        // Find best replica to forward to using proximity-based selection
        if (liveReplicas.isEmpty())
            throw new UnavailableException("No live replicas available for consensus read forwarding", consistencyLevel, 1, 0);

        // Sort by proximity and select the best coordinator
        EndpointsForToken sortedReplicas = DatabaseDescriptor.getNodeProximity().sortedByProximity(localEndpoint, liveReplicas);
        InetAddressAndPort replicaCoordinator = sortedReplicas.get(0).endpoint();

        // Create forward request - consensus reads only have a single command
        ConsensusReadForwardRequest forwardRequest = new ConsensusReadForwardRequest(firstCommand, consistencyLevel);
        Message<ConsensusReadForwardRequest> message = Message.out(CONSENSUS_READ_FORWARD_REQ, forwardRequest);

        try
        {
            // Send synchronous request to replica coordinator
            Object responseObj = MessagingService.instance().sendWithResult(message, replicaCoordinator).get();
            @SuppressWarnings("unchecked")
            Message<CasForwardResponse> responseMessage = (Message<CasForwardResponse>) responseObj;
            CasForwardResponse response = responseMessage.payload;

            // Add warnings from forwarded operation to local ClientWarn
            for (String warning : response.warnings)
                ClientWarn.instance.warn(warning);

            // Check if the forwarded operation had an exception
            if (!response.isSuccess())
                throw response.exception;

            // An absent result means the forwarded read found no partition. Return an empty iterator
            // rather than null, which the caller would read as "no forwarding was needed".
            return new CasForwarding.Forwarded<>(response.hasResult() ? response.partitionIterator()
                                                                      : EmptyIterators.partition());
        }
        catch (CassandraException ce)
        {
            // Rethrow CassandraExceptions from the replica coordinator
            throw ce;
        }
        catch (Exception e)
        {
            throw CasForwarding.readForwardingFailure(e, consistencyLevel,
                                                      consistencyLevel.blockFor(keyspace.getReplicationStrategy()));
        }
    }
}
