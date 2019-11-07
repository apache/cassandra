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

import java.nio.ByteBuffer;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.LegacyLayout.LegacyCell;
import org.apache.cassandra.db.LegacyLayout.LegacyCellName;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.serializers.MarshalException;

import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

public class LegacyLayoutValidationTest
{
    static final String KEYSPACE = "ks";

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static final CFMetaData FIXED = CFMetaData.Builder.create("ks", "cf")
                                                              .addPartitionKey("k", Int32Type.instance)
                                                              .addClusteringColumn("c1", Int32Type.instance)
                                                              .addClusteringColumn("c2", Int32Type.instance)
                                                              .addRegularColumn("v1", Int32Type.instance)
                                                              .addRegularColumn("v2", Int32Type.instance)
                                                              .build();

    private static final CFMetaData COMPACT_FIXED = CFMetaData.Builder.create("ks", "cf", true, false, false)
                                                                      .addPartitionKey("k", Int32Type.instance)
                                                                      .addClusteringColumn("c", Int32Type.instance)
                                                                      .addRegularColumn("v", Int32Type.instance)
                                                                      .build();

    private static final CFMetaData VARIABLE = CFMetaData.Builder.create("ks", "cf")
                                                                 .addPartitionKey("k", Int32Type.instance)
                                                                 .addClusteringColumn("c1", UTF8Type.instance)
                                                                 .addClusteringColumn("c2", UTF8Type.instance)
                                                                 .addRegularColumn("v1", UTF8Type.instance)
                                                                 .addRegularColumn("v2", UTF8Type.instance)
                                                                 .build();

    private static final CFMetaData COMPACT_VARIABLE = CFMetaData.Builder.create("ks", "cf", true, false, false)
                                                                         .addPartitionKey("k", Int32Type.instance)
                                                                         .addClusteringColumn("c", UTF8Type.instance)
                                                                         .addRegularColumn("v", UTF8Type.instance)
                                                                         .build();

    @Test
    public void fixedClusteringSuccess()
    {
        Clustering clustering = Clustering.make(Int32Type.instance.decompose(1), Int32Type.instance.decompose(2));
        ByteBuffer serialized = LegacyLayout.encodeClustering(FIXED, clustering);
        LegacyLayout.decodeClustering(FIXED, serialized);
    }

    @Test (expected = MarshalException.class)
    public void fixedClusteringFailure()
    {
        Clustering clustering = Clustering.make(Int32Type.instance.decompose(1), hexToBytes("07000000000001"));
        ByteBuffer serialized = LegacyLayout.encodeClustering(FIXED, clustering);
        LegacyLayout.decodeClustering(FIXED, serialized);
    }

    @Test
    public void variableClusteringSuccess()
    {
        Clustering clustering = Clustering.make(UTF8Type.instance.decompose("one"), UTF8Type.instance.decompose("two,three"));
        ByteBuffer serialized = LegacyLayout.encodeClustering(VARIABLE, clustering);
        LegacyLayout.decodeClustering(VARIABLE, serialized);
    }

    @Test
    public void fixedCompactClusteringSuccess()
    {
        Clustering clustering = Clustering.make(Int32Type.instance.decompose(2));
        ByteBuffer serialized = LegacyLayout.encodeClustering(COMPACT_FIXED, clustering);
        LegacyLayout.decodeClustering(COMPACT_FIXED, serialized);
    }

    @Test (expected = MarshalException.class)
    public void fixedCompactClusteringFailure()
    {
        Clustering clustering = Clustering.make(hexToBytes("07000000000001"));
        ByteBuffer serialized = LegacyLayout.encodeClustering(COMPACT_FIXED, clustering);
        LegacyLayout.decodeClustering(COMPACT_FIXED, serialized);
    }

    @Test
    public void variableCompactClusteringSuccess()
    {
        Clustering clustering = Clustering.make(UTF8Type.instance.decompose("two,three"));
        ByteBuffer serialized = LegacyLayout.encodeClustering(COMPACT_VARIABLE, clustering);
        LegacyLayout.decodeClustering(COMPACT_VARIABLE, serialized);
    }

    @Test
    public void fixedBoundSuccess()
    {
        Clustering clustering = Clustering.make(Int32Type.instance.decompose(1), Int32Type.instance.decompose(2));
        ByteBuffer serialized = LegacyLayout.encodeClustering(FIXED, clustering);
        LegacyLayout.decodeSliceBound(FIXED, serialized, true);
    }

    @Test (expected = MarshalException.class)
    public void fixedBoundFailure()
    {
        Clustering clustering = Clustering.make(Int32Type.instance.decompose(1), hexToBytes("07000000000001"));
        ByteBuffer serialized = LegacyLayout.encodeClustering(FIXED, clustering);
        LegacyLayout.decodeSliceBound(FIXED, serialized, true);
    }

    @Test
    public void variableBoundSuccess()
    {
        Clustering clustering = Clustering.make(UTF8Type.instance.decompose("one"), UTF8Type.instance.decompose("two,three"));
        ByteBuffer serialized = LegacyLayout.encodeClustering(VARIABLE, clustering);
        LegacyLayout.decodeSliceBound(VARIABLE, serialized, true);
    }

    @Test
    public void fixedCompactBoundSuccess()
    {
        Clustering clustering = Clustering.make(Int32Type.instance.decompose(1));
        ByteBuffer serialized = LegacyLayout.encodeClustering(COMPACT_FIXED, clustering);
        LegacyLayout.decodeSliceBound(COMPACT_FIXED, serialized, true);
    }

    @Test (expected = MarshalException.class)
    public void fixedCompactBoundFailure()
    {
        Clustering clustering = Clustering.make(hexToBytes("07000000000001"));
        ByteBuffer serialized = LegacyLayout.encodeClustering(COMPACT_FIXED, clustering);
        LegacyLayout.decodeSliceBound(COMPACT_FIXED, serialized, true);
    }

    @Test
    public void variableCompactBoundSuccess()
    {
        Clustering clustering = Clustering.make(UTF8Type.instance.decompose("one"));
        ByteBuffer serialized = LegacyLayout.encodeClustering(COMPACT_VARIABLE, clustering);
        LegacyLayout.decodeSliceBound(COMPACT_VARIABLE, serialized, true);
    }

    private static LegacyCell cell(CFMetaData cfm, Clustering clustering, String name, ByteBuffer value) throws UnknownColumnException
    {
        ColumnDefinition definition = cfm.getColumnDefinition(new ColumnIdentifier(name, false));

        ByteBuffer cellName = LegacyCellName.create(clustering, definition).encode(cfm);
        return LegacyCell.regular(cfm, null, cellName, value, 0);

    }

    @Test
    public void fixedValueSuccess() throws Throwable
    {
        DecoratedKey dk = DatabaseDescriptor.getPartitioner().decorateKey(Int32Type.instance.decompose(1000000));
        LegacyLayout.LegacyDeletionInfo deletionInfo = LegacyLayout.LegacyDeletionInfo.live();
        Clustering clustering = Clustering.make(Int32Type.instance.decompose(1), Int32Type.instance.decompose(2));
        Iterator<LegacyCell> cells = Iterators.forArray(cell(FIXED, clustering, "v1", Int32Type.instance.decompose(3)),
                                                        cell(FIXED, clustering, "v2", Int32Type.instance.decompose(4)));
        try (UnfilteredRowIterator iter = LegacyLayout.toUnfilteredRowIterator(FIXED, dk, deletionInfo, cells))
        {
            while (iter.hasNext())
                iter.next();
        }
    }

    @Test (expected = MarshalException.class)
    public void fixedValueFailure() throws Throwable
    {
        DecoratedKey dk = DatabaseDescriptor.getPartitioner().decorateKey(Int32Type.instance.decompose(1000000));
        LegacyLayout.LegacyDeletionInfo deletionInfo = LegacyLayout.LegacyDeletionInfo.live();
        Clustering clustering = Clustering.make(Int32Type.instance.decompose(1), Int32Type.instance.decompose(2));
        Iterator<LegacyCell> cells = Iterators.forArray(cell(FIXED, clustering, "v1", Int32Type.instance.decompose(3)),
                                                        cell(FIXED, clustering, "v2", hexToBytes("0000")));
        try (UnfilteredRowIterator iter = LegacyLayout.toUnfilteredRowIterator(FIXED, dk, deletionInfo, cells))
        {
            while (iter.hasNext())
                iter.next();
        }
    }

    @Test
    public void variableValueSuccess() throws Throwable
    {
        DecoratedKey dk = DatabaseDescriptor.getPartitioner().decorateKey(Int32Type.instance.decompose(1000000));
        LegacyLayout.LegacyDeletionInfo deletionInfo = LegacyLayout.LegacyDeletionInfo.live();
        Clustering clustering = Clustering.make(Int32Type.instance.decompose(1), Int32Type.instance.decompose(2));
        Iterator<LegacyCell> cells = Iterators.forArray(cell(VARIABLE, clustering, "v1", UTF8Type.instance.decompose("3")),
                                                        cell(VARIABLE, clustering, "v2", hexToBytes("0000")));
        try (UnfilteredRowIterator iter = LegacyLayout.toUnfilteredRowIterator(VARIABLE, dk, deletionInfo, cells))
        {
            while (iter.hasNext())
                iter.next();
        }
    }
}
