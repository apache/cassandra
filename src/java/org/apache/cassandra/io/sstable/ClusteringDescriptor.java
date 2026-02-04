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

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.ResizableByteBuffer;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.readUnfilteredClustering;

public class ClusteringDescriptor extends ResizableByteBuffer
{
    private static final byte MAX_START_KIND = (byte) ClusteringBound.MAX_START.kind().ordinal();
    private static final byte MIN_END_KIND = (byte) ClusteringBound.MIN_END.kind().ordinal();

    static final byte EXCL_END_BOUND_CLUSTERING_KIND = (byte) ClusteringPrefix.Kind.EXCL_END_BOUND.ordinal();
    static final byte INCL_START_BOUND_CLUSTERING_KIND = (byte) ClusteringPrefix.Kind.INCL_START_BOUND.ordinal();
    static final byte INCL_END_EXCL_START_BOUNDARY_CLUSTERING_KIND = (byte) ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY.ordinal();

    static final byte STATIC_CLUSTERING_KIND = (byte)ClusteringPrefix.Kind.STATIC_CLUSTERING.ordinal();
    static final byte ROW_CLUSTERING_KIND = (byte) ClusteringPrefix.Kind.CLUSTERING.ordinal();

    static final byte EXCL_END_INCL_START_BOUNDARY_CLUSTERING_KIND = (byte) ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY.ordinal();
    static final byte INCL_END_BOUND_CLUSTERING_KIND = (byte) ClusteringPrefix.Kind.INCL_END_BOUND.ordinal();
    static final byte EXCL_START_BOUND_CLUSTERING_KIND = (byte) ClusteringPrefix.Kind.EXCL_START_BOUND.ordinal();

    protected final AbstractType<?>[] clusteringTypes;
    protected ClusteringPrefix.Kind clusteringKind;
    protected byte clusteringKindEncoded;
    protected int clusteringColumnsBound;

    public ClusteringDescriptor(AbstractType<?>[] clusteringTypes)
    {
        this.clusteringTypes = clusteringTypes;
    }

    protected void loadClustering(RandomAccessReader dataReader, byte clusteringKind, int clusteringColumnsBound) throws IOException
    {
        set(ClusteringPrefix.Kind.values()[clusteringKind], clusteringKind, clusteringColumnsBound);
        if (clusteringKind != STATIC_CLUSTERING_KIND)
            readUnfilteredClustering(dataReader, clusteringTypes, this.clusteringColumnsBound, this);
        else
            resetBuffer();
    }

    public final ClusteringDescriptor resetMinEnd() {
        set(MIN_END_KIND, 0);
        resetBuffer();
        return this;
    }

    public final ClusteringDescriptor resetMaxStart() {
        set(MAX_START_KIND, 0);
        resetBuffer();
        return this;
    }

    public final boolean isMaxStart()
    {
        return clusteringKindEncoded == MAX_START_KIND && clusteringColumnsBound == 0;
    }

    public final void resetClustering()
    {
        set(ClusteringPrefix.Kind.CLUSTERING, 0);

        resetBuffer();
    }

    public final void copy(ClusteringDescriptor newClustering)
    {
        set(newClustering.clusteringKind, newClustering.clusteringColumnsBound());
        overwrite(newClustering.clusteringBytes(), newClustering.clusteringLength());
    }

    private void set(ClusteringPrefix.Kind clusteringKind, int clusteringColumnsBound) {
        set(clusteringKind, (byte) clusteringKind.ordinal(), clusteringColumnsBound);
    }

    private void set(byte clusteringKindEncoded, int clusteringColumnsBound) {
        set(ClusteringPrefix.Kind.values()[clusteringKindEncoded], clusteringKindEncoded, clusteringColumnsBound);
    }

    private void set(ClusteringPrefix.Kind clusteringKind, byte clusteringKindEncoded, int clusteringColumnsBound)
    {
        this.clusteringKindEncoded = clusteringKindEncoded;
        this.clusteringKind = clusteringKind;
        this.clusteringColumnsBound = clusteringColumnsBound;
    }

    // Expose and rename parent data
    public final ByteBuffer clusteringBuffer() {
        return buffer();
    }

    public final int clusteringLength() {
        return length();
    }

    public final byte[] clusteringBytes() {
        return bytes();
    }

    public final AbstractType<?>[] clusteringTypes()
    {
        return clusteringTypes;
    }

    public final byte clusteringKindEncoded() {
        return clusteringKindEncoded;
    }

    public final ClusteringPrefix.Kind clusteringKind() {
        return clusteringKind;
    }

    public final void clusteringKind(ClusteringPrefix.Kind kind)
    {
        clusteringKind = kind;
        clusteringKindEncoded = (byte)kind.ordinal();
    }

    public final int clusteringColumnsBound() {
        return clusteringColumnsBound;
    }

    public final boolean isStartBound()
    {
        return (clusteringKindEncoded == INCL_START_BOUND_CLUSTERING_KIND || clusteringKindEncoded == EXCL_START_BOUND_CLUSTERING_KIND);
    }

    public final boolean isEndBound()
    {
        return (clusteringKindEncoded == INCL_END_BOUND_CLUSTERING_KIND || clusteringKindEncoded == EXCL_END_BOUND_CLUSTERING_KIND);
    }

    public final boolean isBoundary()
    {
        return (clusteringKindEncoded == EXCL_END_INCL_START_BOUNDARY_CLUSTERING_KIND || clusteringKindEncoded == INCL_END_EXCL_START_BOUNDARY_CLUSTERING_KIND);
    }

    public final ClusteringPrefix<?> toClusteringPrefix(List<AbstractType<?>> clusteringTypesList) {
        if (clusteringKindEncoded == ROW_CLUSTERING_KIND) {
            return Clustering.serializer.deserialize(clusteringBuffer(), 0, clusteringTypesList);
        }
        else if (clusteringColumnsBound == 0) {
            return ByteArrayAccessor.factory.bound(clusteringKind);
        }
        else {
            byte[][] values;
            try (DataInputBuffer buffer = new DataInputBuffer(clusteringBuffer(), true))
            {
                values = ClusteringPrefix.serializer.deserializeValuesWithoutSize(buffer, clusteringColumnsBound, 0, clusteringTypesList);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Reading from an in-memory buffer shouldn't trigger an IOException", e);
            }
            return ByteArrayAccessor.factory.boundOrBoundary(clusteringKind, values);
        }
    }
}
