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

import java.io.IOException;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.DataInputPlus;

/**
 * Helper class to deserialize Unfiltered object from disk efficiently.
 *
 * More precisely, this class is used by the low-level reader to ensure
 * we don't do more work than necessary (i.e. we don't allocate/deserialize
 * objects for things we don't care about).
 */
public class UnfilteredDeserializer
{
    protected final TableMetadata metadata;
    protected final DataInputPlus in;
    protected final DeserializationHelper helper;

    private final ClusteringPrefix.Deserializer clusteringDeserializer;
    private final SerializationHeader header;

    private int nextFlags;
    private int nextExtendedFlags;
    private boolean isReady;
    private boolean isDone;

    private final Row.Builder builder;

    private UnfilteredDeserializer(TableMetadata metadata,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9499
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12716
                                   DataInputPlus in,
                                   SerializationHeader header,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15389
                                   DeserializationHelper helper)
    {
        this.metadata = metadata;
        this.in = in;
        this.helper = helper;
        this.header = header;
        this.clusteringDeserializer = new ClusteringPrefix.Deserializer(metadata.comparator, in, header);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10193
        this.builder = BTreeRow.sortedBuilder();
    }

    public static UnfilteredDeserializer create(TableMetadata metadata,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9499
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9499
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9499
                                                DataInputPlus in,
                                                SerializationHeader header,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15389
                                                DeserializationHelper helper)
    {
        return new UnfilteredDeserializer(metadata, in, header, helper);
    }

    /**
     * Whether or not there is more atom to read.
     */
    public boolean hasNext() throws IOException
    {
        if (isReady)
            return true;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10750

        prepareNext();
        return !isDone;
    }

    private void prepareNext() throws IOException
    {
        if (isDone)
            return;

        nextFlags = in.readUnsignedByte();
        if (UnfilteredSerializer.isEndOfPartition(nextFlags))
        {
            isDone = true;
            isReady = false;
            return;
        }

        nextExtendedFlags = UnfilteredSerializer.readExtendedFlags(in, nextFlags);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10378

        clusteringDeserializer.prepare(nextFlags, nextExtendedFlags);
        isReady = true;
    }

    /**
     * Compare the provided bound to the next atom to read on disk.
     *
     * This will not read/deserialize the whole atom but only what is necessary for the
     * comparison. Whenever we know what to do with this atom (read it or skip it),
     * readNext or skipNext should be called.
     */
    public int compareNextTo(ClusteringBound bound) throws IOException
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12716
        if (!isReady)
            prepareNext();

        assert !isDone;

        return clusteringDeserializer.compareNextTo(bound);
    }

    /**
     * Returns whether the next atom is a row or not.
     */
    public boolean nextIsRow() throws IOException
    {
        if (!isReady)
            prepareNext();

        return UnfilteredSerializer.kind(nextFlags) == Unfiltered.Kind.ROW;
    }

    /**
     * Returns the next atom.
     */
    public Unfiltered readNext() throws IOException
    {
        isReady = false;
        if (UnfilteredSerializer.kind(nextFlags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11213
            ClusteringBoundOrBoundary bound = clusteringDeserializer.deserializeNextBound();
            return UnfilteredSerializer.serializer.deserializeMarkerBody(in, header, bound);
        }
        else
        {
            builder.newRow(clusteringDeserializer.deserializeNextClustering());
            return UnfilteredSerializer.serializer.deserializeRowBody(in, header, helper, nextFlags, nextExtendedFlags, builder);
        }
    }

    /**
     * Clears any state in this deserializer.
     */
    public void clearState()
    {
        isReady = false;
        isDone = false;
    }

    /**
     * Skips the next atom.
     */
    public void skipNext() throws IOException
    {
        isReady = false;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10378
        clusteringDeserializer.skipNext();
        if (UnfilteredSerializer.kind(nextFlags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
        {
            UnfilteredSerializer.serializer.skipMarkerBody(in);
        }
        else
        {
            UnfilteredSerializer.serializer.skipRowBody(in);
        }
    }
}
