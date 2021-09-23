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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A tracker objects that can be registered against {@link StorageProxy} to be called back with information on executed
 * queries.
 *
 * <p>The goal of this interface is to provide to implementations enough information for it to accurately estimate how
 * much "work" a query has performed. So while for write this mostly just mean passing the generated mutations, for
 * reads this mean passing the unfiltered result of the query.
 *
 * <p>The methods of this tracker are called from {@link StorageProxy} and are thus "coordinator level". As such, all
 * user writes or reads will trigger the call of one of these methods, as will internal distributed system table
 * queries, but internal local system table queries will not.
 *
 * <p>For writes, the {@link #onWrite} method is only called for the "user write", but if that write trigger either
 * secondary index or materialized views updates, those additional update do not trigger additional calls.
 *
 * <p>The methods of this tracker are called on hot path, so  they should be as lightweight as possible.
 */
public interface QueryInfoTracker
{
    /**
     * Called before every (non-LWT) write coordinated on the local node.
     *
     * @param state            the state of the client that performed the write
     * @param isLogged         whether this is a logged batch write.
     * @param mutations        the mutations written by the write.
     * @param consistencyLevel the consistency level of the write.
     * @return a tracker that should be notified when either the read error out or completes successfully.
     */
    WriteTracker onWrite(ClientState state,
                         boolean isLogged,
                         Collection<? extends IMutation> mutations,
                         ConsistencyLevel consistencyLevel);

    /**
     * Called before every non-range read coordinated on the local node.
     *
     * @param state            the state of the client that performed the read
     * @param table            the metadata for the table read.
     * @param commands         the commands for the read performed.
     * @param consistencyLevel the consistency level of the read.
     * @return a tracker that should be notified when either the read error out or completes successfully.
     */
    ReadTracker onRead(ClientState state,
                       TableMetadata table,
                       List<SinglePartitionReadCommand> commands,
                       ConsistencyLevel consistencyLevel);

    /**
     * Called before every range read coordinated on the local node.
     *
     * @param state            the state of the client that performed the range read
     * @param table            the metadata for the table read.
     * @param command          the command for the read performed.
     * @param consistencyLevel the consistency level of the read.
     * @return a tracker that should  be notified when either the read error out or completes successfully.
     */
    ReadTracker onRangeRead(ClientState state,
                            TableMetadata table,
                            PartitionRangeReadCommand command,
                            ConsistencyLevel consistencyLevel);

    /**
     * Called before every LWT coordinated by the local node.
     *
     * @param state             the state of the client that performed the LWT
     * @param table             the metadata of the table on which the LWT applies.
     * @param key               the partition key on which the LWT operates.
     * @param serialConsistency the serial consistency of the LWT.
     * @param commitConsistency the commit consistency of the LWT.
     * @return a {@link LWTWriteTracker} objects whose methods are called as part of the LWT execution.
     */
    LWTWriteTracker onLWTWrite(ClientState state,
                               TableMetadata table,
                               DecoratedKey key,
                               ConsistencyLevel serialConsistency,
                               ConsistencyLevel commitConsistency);

    /**
     * A tracker that does nothing.
     */
    QueryInfoTracker NOOP = new QueryInfoTracker()
    {
        @Override
        public WriteTracker onWrite(ClientState state,
                                    boolean isLogged,
                                    Collection<? extends IMutation> mutations,
                                    ConsistencyLevel consistencyLevel)
        {
            return WriteTracker.NOOP;
        }

        @Override
        public ReadTracker onRead(ClientState state,
                                  TableMetadata table,
                                  List<SinglePartitionReadCommand> commands,
                                  ConsistencyLevel consistencyLevel)
        {
            return ReadTracker.NOOP;
        }

        @Override
        public ReadTracker onRangeRead(ClientState state,
                                       TableMetadata table,
                                       PartitionRangeReadCommand command,
                                       ConsistencyLevel consistencyLevel)
        {
            return ReadTracker.NOOP;
        }

        @Override
        public LWTWriteTracker onLWTWrite(ClientState state,
                                          TableMetadata table,
                                          DecoratedKey key,
                                          ConsistencyLevel serialConsistency,
                                          ConsistencyLevel commitConsistency)
        {
            return LWTWriteTracker.NOOP;
        }
    };

    /**
     * A tracker for a specific query.
     *
     * <p>For the tracked query, exactly one of its method should be called.
     */
    interface Tracker
    {
        /**
         * Called when the tracked query completes successfully.
         */
        void onDone();

        /**
         * Called when the tracked query completes with an error.
         */
        void onError(Throwable exception);
    }

    /**
     * Tracker for a write query.
     */
    interface WriteTracker extends Tracker
    {
        WriteTracker NOOP = new WriteTracker()
        {
            @Override
            public void onDone()
            {
            }

            @Override
            public void onError(Throwable exception)
            {
            }
        };
    }

    /**
     * Tracker for a read query.
     */
    interface ReadTracker extends Tracker
    {
        /**
         * Calls just before queries are sent with the contacts from the replica plan.
         * Note that this callback method may be invoked more than once for a given read,
         * e.g. range quries spanning multiple partitions are internally issued as a
         * number of subranges requests to different replicas (with different
         * ReplicaPlans). This callback is called at least once for a given read.
         *
         * @param replicaPlan the queried nodes.
         */
        void onReplicaPlan(ReplicaPlan.ForRead<?> replicaPlan);

        /**
         * Called on every new reconciled partition.
         *
         * @param partitionKey the partition key.
         */
        void onPartition(DecoratedKey partitionKey);

        /**
         * Called on every row read.
         *
         * @param row          the merged row.
         */
        void onRow(Row row);

        ReadTracker NOOP = new ReadTracker()
        {
            @Override
            public void onReplicaPlan(ReplicaPlan.ForRead<?> replicaPlan)
            {
            }

            @Override
            public void onPartition(DecoratedKey partitionKey)
            {
            }

            @Override
            public void onRow(Row row)
            {
            }

            @Override
            public void onDone()
            {
            }

            @Override
            public void onError(Throwable exception)
            {
            }
        };
    }

    /**
     * Tracker for LWTs, used to get information on the actual work done by the LWT.
     *
     * <p>For a given LWT, the tracker created by {@link #onLWTWrite} will first have its read
     * methods called. Then, based on that read result and the LWT conditions, either the {@link #onNotApplied()} or
     * the {@link #onApplied} method will be called.
     */
    interface LWTWriteTracker extends ReadTracker
    {
        /**
         * Called if the LWT this is tracking does not applies (it's condition evaluates to {@code false}).
         */
        void onNotApplied();

        /**
         * Called if the LWT this is tracking does applies.
         *
         * @param update the update that is committed by the LWT.
         */
        void onApplied(PartitionUpdate update);

        /**
         * A tracker that does nothing.
         */
        LWTWriteTracker NOOP = new LWTWriteTracker()
        {
            @Override
            public void onReplicaPlan(ReplicaPlan.ForRead<?> replicaPlan)
            {
            }

            @Override
            public void onPartition(DecoratedKey partitionKey)
            {
            }

            @Override
            public void onRow(Row row)
            {
            }

            @Override
            public void onNotApplied()
            {
            }

            @Override
            public void onApplied(PartitionUpdate update)
            {
            }

            @Override
            public void onDone()
            {
            }

            @Override
            public void onError(Throwable exception)
            {
            }
        };

    }
}
