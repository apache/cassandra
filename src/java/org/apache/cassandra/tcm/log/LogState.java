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
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Period;
import org.apache.cassandra.tcm.Sealed;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * A {@code LogState} represents the state of a cluster metadata log as a base state {@code ClusterMetadata)
 * and a series of transformations applied to that base state.
 */
public class LogState
{
    private static final Logger logger = LoggerFactory.getLogger(LogState.class);
    public static LogState EMPTY = new LogState(null, Replication.EMPTY);
    public static final IVersionedSerializer<LogState> defaultMessageSerializer = new Serializer(NodeVersion.CURRENT.serializationVersion());

    private static volatile Serializer serializerCache;
    public static IVersionedSerializer<LogState> messageSerializer(Version version)
    {
        Serializer cached = serializerCache;
        if (cached != null && cached.serializationVersion.equals(version))
            return cached;
        cached = new Serializer(version);
        serializerCache = cached;
        return cached;
    }

    /**
     * The base state.
     */
    public final ClusterMetadata baseState;

    /**
     * The log entries containing the transformations that need to be applied to the base state to recreate the final state.
     */
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

    public Epoch latestEpoch()
    {
        if (transformations.isEmpty())
        {
            if (baseState == null)
                return Epoch.EMPTY;
            return baseState.epoch;
        }
        return transformations.latestEpoch();
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
     * Contains the logic for generating a LogState from an arbitrary epoch to the current epoch.
     * The LogState returned is suitable for bringing a metadata log up to date with the current state as viewed from
     * this local log. This method will attempt to minimise the number of individual log entries contained in the
     * LogState. If any snapshots with a higher epoch than the supplied start point, the LogState will include the most
     * recent along with any subsequent log entries.
     * Callers supply:
     *  * The epoch to act as the starting point for the LogState. If no metadata snapshot for a higher epoch
     *    exists, the LogState will contain all log entries with an epoch greater than this. If such a snapshot
     *    is found, it will form the baseState of the LogState, and only log entries with epochs greater than the
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
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Could not restore the state.", t);
            throw new RuntimeException(t);
        }
    }

    /**
     * Contains the logic for generating a LogState up to an arbitrary epoch.
     * The LogState returned is suitable for point in time recovery of cluster metadata. If any snapshots are available
     * which precede the target epoch, the LogState will include the one with the highest epoch along with subsequent
     * entries up to and including the target epoch.
     * Callers supply:
     *  * The epoch to act as the termination point for the LogState. If no metadata snapshot for a lower epoch
     *    exists, the LogState will contain all log entries with an epoch less than this. If such a snapshot
     *    is found, it will form the baseState of the LogState, and only log entries with epochs greater than the
     *    snapshot's and less than or equal to the target will be included.
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
    public static LogState getForRecovery(Epoch target)
    {
        LogStorage logStorage = LogStorage.SystemKeyspace;
        MetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
        Sealed sealed = Sealed.lookupForReplication(target);
        // a snapshot exists for exactly the epoch we're looking for
        if (sealed.epoch.is(target))
            return LogState.make(snapshots.getSnapshot(sealed.epoch));

        Sealed preceding;
        ClusterMetadata base;
        if (sealed.epoch.isAfter(target))
        {
            // we need the snapshot from the preceding period plus some entries. Scan result includes the supplied
            // start period so we have to either manually decrement the start period (or fetch a list of up to 2 items)
            List<Sealed> before = Period.scanLogForRecentlySealed(SystemKeyspace.LocalMetadataLog, sealed.period - 1, 1);
            assert !before.isEmpty() : "No earlier snapshot found, started looking at " + (sealed.period - 1) + " target = " + target;
            preceding = before.get(0);
            base = snapshots.getSnapshot(preceding.epoch);
        }
        else
        {
            // scan from the start of the log table - expensive
            preceding = new Sealed(Period.FIRST, Epoch.EMPTY);
            base = new ClusterMetadata(DatabaseDescriptor.getPartitioner());
        }

        // TODO add LogStorage.getReplication(startPeriod, startEpoch, endEpoch); so we don't have to overfetch
        Replication allSince = logStorage.getReplication(preceding.period, preceding.epoch);
        List<Entry> entries = new ArrayList<>();
        for (Entry e : allSince.entries())
        {
            if (e.epoch.isAfter(target))
                break;
            entries.add(e);
        }
        return LogState.make(base, Replication.of(entries));
    }

    static final class Serializer implements IVersionedSerializer<LogState>
    {
        private final Version serializationVersion;

        public Serializer(Version serializationVersion)
        {
            this.serializationVersion = serializationVersion;
        }

        @Override
        public void serialize(LogState t, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(t.baseState != null);
            if (t.baseState != null)
                VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, t.baseState, out, serializationVersion);
            VerboseMetadataSerializer.serialize(Replication.serializer, t.transformations, out, serializationVersion);
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
                size += VerboseMetadataSerializer.serializedSize(ClusterMetadata.serializer, t.baseState, serializationVersion);
            size += VerboseMetadataSerializer.serializedSize(Replication.serializer, t.transformations, serializationVersion);
            return size;
        }
    }
}
