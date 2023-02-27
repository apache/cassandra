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

package org.apache.cassandra.tcm.log;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;

public class LogState
{
    private static final Logger logger = LoggerFactory.getLogger(LogState.class);
    public static LogState EMPTY = new LogState(null, Replication.EMPTY);
    public static final Serializer serializer = new Serializer();

    public final ClusterMetadata baseState;
    public final Replication transformations;

    // Uses Replication rather than an just a list of entries primarily to avoid duplicating the existing serializer
    public LogState(ClusterMetadata baseState, Replication transformations)
    {
        this.baseState = baseState;
        this.transformations = transformations;
    }

    public static LogState make(ClusterMetadata baseState, Replication increments)
    {
        return new LogState(baseState, increments);
    }

    public static LogState make(Replication increments)
    {
        return new LogState(null, increments);
    }

    public static LogState make(ClusterMetadata baseState)
    {
        return new LogState(baseState, Replication.EMPTY);
    }

    public boolean isEmpty()
    {
        return baseState == null && transformations.isEmpty();
    }

    @Override
    public String toString()
    {
        return "LogState{" +
               "baseState=" + (baseState != null ? baseState.epoch.toString() : "none ") +
               ", transformations=" + transformations.toString() +
               '}';
    }

    /**
     * Contains the logic for generating a LogState from an arbitrary Epoch.
     * Callers supply:
     *  * The epoch to act as the starting point for the LogState. If no metadata snapshot for a higher epoch
     *    exists, the LogState will contain all log entries with an epoch greater than this. If such a snapshot
     *    is found, it will form the baseState of the LogState, and only log entries with epoch's greater than the
     *    snapshot's will be included.
     *  * MetadataSnapshots to provide access to serialized metadata snapshots. Outside of tests, the only
     *    implementation of this is SystemKeyspaceMetadataSnapshots which persists the info in local system tables.
     *    Both the metadata_last_sealed_period and metadata_snapshot tables are populated by the after commit hook of a
     *    SealPeriod transform, so are neither atomically updated nor guaranteed to completely up to date. This is ok,
     *    though as this method can handle both being missing or out of date by degrading to a slower lookup.
     *  * A LogReader to return a list of log entries in the form of a Replication given a starting epoch and
     *    optionally a period if one can be derived from a snapshot. If no snapshot is available, the LogReader
     *    should determine the starting period itself (typically by checking the system.sealed_periods table, but
     *    falling back to a full log scan in the pathological case) and will read log entries from either the local or
     *    distributed log table depending on the calling context.
     * @param since
     * @param snapshots
     * @param reader
     * @return
     */
    @VisibleForTesting
    public static LogState getLogState(Epoch since, MetadataSnapshots snapshots, LogReader reader)
    {
        try
        {
            ClusterMetadata snapshot = snapshots.getLatestSnapshotAfter(since);
            if (snapshot == null)
            {
                logger.info("No suitable metadata snapshot found, not including snapshot in LogState since {}", since);
                return new LogState(null, reader.getReplication(since));
            }
            else
            {
                    logger.info("Loaded snapshot of epoch {} from sealed period {} which follows requested start epoch, including in LogState since {}",
                                snapshot.epoch, snapshot.period, since.getEpoch());
                    return new LogState(snapshot, reader.getReplication(snapshot.nextPeriod(), snapshot.epoch));
            }
        }
        catch (Throwable t)
        {
            logger.error("Could not restore the state.", t);
            throw new RuntimeException(t);
        }
    }

    static final class Serializer implements IVersionedSerializer<LogState>
    {
        @Override
        public void serialize(LogState t, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(t.baseState != null);
            if (t.baseState != null)
                VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, t.baseState, out);
            VerboseMetadataSerializer.serialize(Replication.serializer, t.transformations, out);
        }

        @Override
        public LogState deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean hasSnapshot = in.readBoolean();
            ClusterMetadata snapshot = null;
            if (hasSnapshot)
                snapshot = VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer, in);
            Replication replication = VerboseMetadataSerializer.deserialize(Replication.serializer, in);
            return new LogState(snapshot, replication);
        }

        @Override
        public long serializedSize(LogState t, int version)
        {
            long size = TypeSizes.BOOL_SIZE;
            if (t.baseState != null)
                size += VerboseMetadataSerializer.serializedSize(ClusterMetadata.serializer, t.baseState);
            size += VerboseMetadataSerializer.serializedSize(Replication.serializer, t.transformations);
            return size;
        }
    }
}
