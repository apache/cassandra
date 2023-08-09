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
package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.Term.MultiItemTerminal;
import org.apache.cassandra.cql3.statements.Bound;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusteringColumnRestrictionsTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testBoundsAsClusteringWithNoRestrictions()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC);

        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));
    }

    /**
     * Test 'clustering_0 = 1' with only one clustering column
     */
    @Test
    public void testBoundsAsClusteringWithOneEqRestrictionsAndOneClusteringColumn()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC);

        ByteBuffer clustering_0 = ByteBufferUtil.bytes(1);
        Restriction eq = newSingleEq(tableMetadata, 0, clustering_0);

        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, eq);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, clustering_0);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, clustering_0);
    }

    /**
     * Test 'clustering_1 = 1' with 2 clustering columns
     */
    @Test
    public void testBoundsAsClusteringWithOneEqRestrictionsAndTwoClusteringColumns()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC);

        ByteBuffer clustering_0 = ByteBufferUtil.bytes(1);
        Restriction eq = newSingleEq(tableMetadata, 0, clustering_0);

        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, eq);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, clustering_0);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, clustering_0);
    }

    /**
     * Test 'clustering_0 IN (1, 2, 3)' with only one clustering column
     */
    @Test
    public void testBoundsAsClusteringWithOneInRestrictionsAndOneClusteringColumn()
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);

        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC);

        Restriction in = newSingleIN(tableMetadata, 0, value1, value2, value3);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, in);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), true, value2);
        assertStartBound(get(bounds, 2), true, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertEndBound(get(bounds, 0), true, value1);
        assertEndBound(get(bounds, 1), true, value2);
        assertEndBound(get(bounds, 2), true, value3);
    }

    /**
     * Test slice restriction (e.g 'clustering_0 > 1') with only one clustering column
     */
    @Test
    public void testBoundsAsClusteringWithSliceRestrictionsAndOneClusteringColumn()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        Restriction slice = newSingleSlice(tableMetadata, 0, Bound.START, false, value1);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, slice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        slice = newSingleSlice(tableMetadata, 0, Bound.START, true, value1);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        slice = newSingleSlice(tableMetadata, 0, Bound.END, true, value1);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1);

        slice = newSingleSlice(tableMetadata, 0, Bound.END, false, value1);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);

        slice = newSingleSlice(tableMetadata, 0, Bound.START, false, value1);
        Restriction slice2 = newSingleSlice(tableMetadata, 0, Bound.END, false, value2);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value2);

        slice = newSingleSlice(tableMetadata, 0, Bound.START, true, value1);
        slice2 = newSingleSlice(tableMetadata, 0, Bound.END, true, value2);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value2);
    }

    /**
     * Test slice restriction (e.g 'clustering_0 > 1') with only one descending clustering column
     */
    @Test
    public void testBoundsAsClusteringWithSliceRestrictionsAndOneDescendingClusteringColumn()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.DESC, Sort.DESC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        Restriction slice = newSingleSlice(tableMetadata, 0, Bound.START, false, value1);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, slice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);

        slice = newSingleSlice(tableMetadata, 0, Bound.START, true, value1);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1);

        slice = newSingleSlice(tableMetadata, 0, Bound.END, true, value1);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        slice = newSingleSlice(tableMetadata, 0, Bound.END, false, value1);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        slice = newSingleSlice(tableMetadata, 0, Bound.START, false, value1);
        Restriction slice2 = newSingleSlice(tableMetadata, 0, Bound.END, false, value2);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);

        slice = newSingleSlice(tableMetadata, 0, Bound.START, true, value1);
        slice2 = newSingleSlice(tableMetadata, 0, Bound.END, true, value2);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1);
    }

    /**
     * Test 'clustering_0 = 1 AND clustering_1 IN (1, 2, 3)'
     */
    @Test
    public void testBoundsAsClusteringWithEqAndInRestrictions()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        Restriction eq = newSingleEq(tableMetadata, 0, value1);
        Restriction in = newSingleIN(tableMetadata, 1, value1, value2, value3);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, eq, in);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value1);
        assertStartBound(get(bounds, 1), true, value1, value2);
        assertStartBound(get(bounds, 2), true, value1, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value1);
        assertEndBound(get(bounds, 1), true, value1, value2);
        assertEndBound(get(bounds, 2), true, value1, value3);
    }

    /**
     * Test equal and slice restrictions (e.g 'clustering_0 = 0 clustering_1 > 1')
     */
    @Test
    public void testBoundsAsClusteringWithEqAndSliceRestrictions()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);

        Restriction eq = newSingleEq(tableMetadata, 0, value3);

        Restriction slice = newSingleSlice(tableMetadata, 1, Bound.START, false, value1);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, eq, slice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value3, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value3);

        slice = newSingleSlice(tableMetadata, 1, Bound.START, true, value1);
        restrictions = restrictions(tableMetadata, eq, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value3, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value3);

        slice = newSingleSlice(tableMetadata, 1, Bound.END, true, value1);
        restrictions =  restrictions(tableMetadata, eq, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value3, value1);

        slice = newSingleSlice(tableMetadata, 1, Bound.END, false, value1);
        restrictions =  restrictions(tableMetadata, eq, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value3, value1);

        slice = newSingleSlice(tableMetadata, 1, Bound.START, false, value1);
        Restriction slice2 = newSingleSlice(tableMetadata, 1, Bound.END, false, value2);
        restrictions =  restrictions(tableMetadata, eq, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value3, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value3, value2);

        slice = newSingleSlice(tableMetadata, 1, Bound.START, true, value1);
        slice2 = newSingleSlice(tableMetadata, 1, Bound.END, true, value2);
        restrictions =  restrictions(tableMetadata, eq, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value3, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value3, value2);
    }

    /**
     * Test '(clustering_0, clustering_1) = (1, 2)' with two clustering column
     */
    @Test
    public void testBoundsAsClusteringWithMultiEqRestrictions()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        Restriction eq = newMultiEq(tableMetadata, 0, value1, value2);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, eq);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);
    }

    /**
     * Test '(clustering_0, clustering_1) IN ((1, 2), (2, 3))' with two clustering column
     */
    @Test
    public void testBoundsAsClusteringWithMultiInRestrictions()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        Restriction in = newMultiIN(tableMetadata, 0, asList(value1, value2), asList(value2, value3));
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, in);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);
        assertStartBound(get(bounds, 1), true, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);
        assertEndBound(get(bounds, 1), true, value2, value3);
    }

    /**
     * Test multi-column slice restrictions (e.g '(clustering_0) > (1)') with only one clustering column
     */
    @Test
    public void testBoundsAsClusteringWithMultiSliceRestrictionsWithOneClusteringColumn()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC);


        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        Restriction slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, slice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        slice = newMultiSlice(tableMetadata, 0, Bound.END, true, value1);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1);

        slice = newMultiSlice(tableMetadata, 0, Bound.END, false, value1);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);

        slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1);
        Restriction slice2 = newMultiSlice(tableMetadata, 0, Bound.END, false, value2);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value2);

        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1);
        slice2 = newMultiSlice(tableMetadata, 0, Bound.END, true, value2);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value2);
    }

    /**
     * Test multi-column slice restrictions (e.g '(clustering_0) > (1)') with only one clustering column in reverse
     * order
     */
    @Test
    public void testBoundsAsClusteringWithMultiSliceRestrictionsWithOneDescendingClusteringColumn()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.DESC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        Restriction slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1);
        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, slice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);

        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1);

        slice = newMultiSlice(tableMetadata, 0, Bound.END, true, value1);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        slice = newMultiSlice(tableMetadata, 0, Bound.END, false, value1);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1);
        Restriction slice2 = newMultiSlice(tableMetadata, 0, Bound.END, false, value2);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);

        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1);
        slice2 = newMultiSlice(tableMetadata, 0, Bound.END, true, value2);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1);
    }

    /**
     * Test multi-column slice restrictions (e.g '(clustering_0, clustering_1) > (1, 2)')
     */
    @Test
    public void testBoundsAsClusteringWithMultiSliceRestrictionsWithTwoClusteringColumn()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        // (clustering_0, clustering1) > (1, 2)
        Restriction slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1, value2);
        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, slice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        // (clustering_0, clustering1) >= (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        // (clustering_0, clustering1) <= (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.END, true, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);

        // (clustering_0, clustering1) < (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.END, false, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);

        // (clustering_0, clustering1) > (1, 2) AND (clustering_0) < (2)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1, value2);
        Restriction slice2 = newMultiSlice(tableMetadata, 0, Bound.END, false, value2);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value2);

        // (clustering_0, clustering1) >= (1, 2) AND (clustering_0, clustering1) <= (2, 1)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2);
        slice2 = newMultiSlice(tableMetadata, 0, Bound.END, true, value2, value1);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value2, value1);
    }

    /**
     * Test multi-column slice restrictions with 2 descending clustering columns (e.g '(clustering_0, clustering_1) > (1, 2)')
     */
    @Test
    public void testBoundsAsClusteringWithMultiSliceRestrictionsWithTwoDescendingClusteringColumns()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.DESC, Sort.DESC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        // (clustering_0, clustering1) > (1, 2)
        Restriction slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1, value2);
        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, slice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);

        // (clustering_0, clustering1) >= (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);

        // (clustering_0, clustering1) <= (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.END, true, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        // (clustering_0, clustering1) < (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.END, false, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));


        // (clustering_0, clustering1) > (1, 2) AND (clustering_0) < (2)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1, value2);
        Restriction slice2 = newMultiSlice(tableMetadata, 0, Bound.END, false, value2);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);

        // (clustering_0, clustering1) >= (1, 2) AND (clustering_0, clustering1) <= (2, 1)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2);
        slice2 = newMultiSlice(tableMetadata, 0, Bound.END, true, value2, value1);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value2, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);
    }

    /**
     * Test multi-column slice restrictions with 1 descending clustering column and 1 ascending
     * (e.g '(clustering_0, clustering_1) > (1, 2)')
     */
    @Test
    public void testBoundsAsClusteringWithMultiSliceRestrictionsWithOneDescendingAndOneAscendingClusteringColumns()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.DESC, Sort.ASC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        // (clustering_0, clustering1) > (1, 2)
        Restriction slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1, value2);
        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, slice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEmptyStart(get(bounds, 0));
        assertStartBound(get(bounds, 1), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);
        assertEndBound(get(bounds, 1), true, value1);

        // (clustering_0, clustering1) >= (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEmptyStart(get(bounds, 0));
        assertStartBound(get(bounds, 1), true, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);
        assertEndBound(get(bounds, 1), true, value1);

        // (clustering_0, clustering1) <= (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.END, true, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);
        assertEmptyEnd(get(bounds, 1));

        // (clustering_0, clustering1) < (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.END, false, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);
        assertEmptyEnd(get(bounds, 1));

        // (clustering_0, clustering1) > (1, 2) AND (clustering_0) < (2)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1, value2);
        Restriction slice2 = newMultiSlice(tableMetadata, 0, Bound.END, false, value2);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), false, value2);
        assertStartBound(get(bounds, 1), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);
        assertEndBound(get(bounds, 1), true, value1);

        // (clustering_0) > (1) AND (clustering_0, clustering1) < (2, 1)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1);
        slice2 = newMultiSlice(tableMetadata, 0, Bound.END, false, value2, value1);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value2);
        assertStartBound(get(bounds, 1), false, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value2, value1);
        assertEndBound(get(bounds, 1), false, value1);

        // (clustering_0, clustering1) >= (1, 2) AND (clustering_0, clustering1) <= (2, 1)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2);
        slice2 = newMultiSlice(tableMetadata, 0, Bound.END, true, value2, value1);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertStartBound(get(bounds, 0), true, value2);
        assertStartBound(get(bounds, 1), false, value2);
        assertStartBound(get(bounds, 2), true, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertEndBound(get(bounds, 0), true, value2, value1);
        assertEndBound(get(bounds, 1), false, value1);
        assertEndBound(get(bounds, 2), true, value1);
    }

    /**
     * Test multi-column slice restrictions with 1 descending clustering column and 1 ascending
     * (e.g '(clustering_0, clustering_1) > (1, 2)')
     */
    @Test
    public void testBoundsAsClusteringWithMultiSliceRestrictionsWithOneAscendingAndOneDescendingClusteringColumns()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.DESC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        // (clustering_0, clustering1) > (1, 2)
        Restriction slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1, value2);
        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, slice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);
        assertEmptyEnd(get(bounds, 1));

        // (clustering_0, clustering1) >= (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);
        assertEmptyEnd(get(bounds, 1));

        // (clustering_0, clustering1) <= (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.END, true, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEmptyStart(get(bounds, 0));
        assertStartBound(get(bounds, 1), true, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);
        assertEndBound(get(bounds, 1), true, value1);

        // (clustering_0, clustering1) < (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.END, false, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEmptyStart(get(bounds, 0));
        assertStartBound(get(bounds, 1), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);
        assertEndBound(get(bounds, 1), true, value1);

        // (clustering_0, clustering1) > (1, 2) AND (clustering_0) < (2)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1, value2);
        Restriction slice2 = newMultiSlice(tableMetadata, 0, Bound.END, false, value2);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);
        assertEndBound(get(bounds, 1), false, value2);

        // (clustering_0, clustering1) >= (1, 2) AND (clustering_0, clustering1) <= (2, 1)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2);
        slice2 = newMultiSlice(tableMetadata, 0, Bound.END, true, value2, value1);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), false, value1);
        assertStartBound(get(bounds, 2), true, value2, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);
        assertEndBound(get(bounds, 1), false, value2);
        assertEndBound(get(bounds, 2), true, value2);
    }

    /**
     * Test multi-column slice restrictions with 2 ascending clustering column and 2 descending
     * (e.g '(clustering_0, clustering1, clustering_3, clustering4) > (1, 2, 3, 4)')
     */
    @Test
    public void testBoundsAsClusteringWithMultiSliceRestrictionsWithTwoAscendingAndTwoDescendingClusteringColumns()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC, Sort.DESC, Sort.DESC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);

        // (clustering_0, clustering1, clustering_2, clustering_3) > (1, 2, 3, 4)
        Restriction slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1, value2, value3, value4);
        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, slice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);
        assertStartBound(get(bounds, 1), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2, value3, value4);
        assertEmptyEnd(get(bounds, 1));

        // clustering_0 = 1 AND (clustering_1, clustering_2, clustering_3) > (2, 3, 4)
        Restriction eq = newSingleEq(tableMetadata, 0, value1);
        slice = newMultiSlice(tableMetadata, 1, Bound.START, false, value2, value3, value4);
        restrictions = restrictions(tableMetadata, slice, eq);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);
        assertStartBound(get(bounds, 1), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2, value3, value4);
        assertEndBound(get(bounds, 1), true, value1);

        // clustering_0 IN (1, 2) AND (clustering_1, clustering_2, clustering_3) > (2, 3, 4)
        Restriction in = newSingleIN(tableMetadata, 0, value1, value2);
        slice = newMultiSlice(tableMetadata, 1, Bound.START, false, value2, value3, value4);
        restrictions = restrictions(tableMetadata, slice, in);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(4, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);
        assertStartBound(get(bounds, 1), false, value1, value2);
        assertStartBound(get(bounds, 2), true, value2, value2);
        assertStartBound(get(bounds, 3), false, value2, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(4, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2, value3, value4);
        assertEndBound(get(bounds, 1), true, value1);
        assertEndBound(get(bounds, 2), false, value2, value2, value3, value4);
        assertEndBound(get(bounds, 3), true, value2);

        // (clustering_0, clustering1) >= (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        // (clustering_0, clustering1, clustering_2, clustering_3) >= (1, 2, 3, 4)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2, value3, value4);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);
        assertStartBound(get(bounds, 1), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3, value4);
        assertEmptyEnd(get(bounds, 1));

        // (clustering_0, clustering1, clustering_2, clustering_3) <= (1, 2, 3, 4)
        slice = newMultiSlice(tableMetadata, 0, Bound.END, true, value1, value2, value3, value4);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEmptyStart(get(bounds, 0));
        assertStartBound(get(bounds, 1), true, value1, value2, value3, value4);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);
        assertEndBound(get(bounds, 1), true, value1, value2);

        // (clustering_0, clustering1, clustering_2, clustering_3) < (1, 2, 3, 4)
        slice = newMultiSlice(tableMetadata, 0, Bound.END, false, value1, value2, value3, value4);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEmptyStart(get(bounds, 0));
        assertStartBound(get(bounds, 1), false, value1, value2, value3, value4);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);
        assertEndBound(get(bounds, 1), true, value1, value2);

        // (clustering_0, clustering1, clustering_2, clustering_3) > (1, 2, 3, 4) AND (clustering_0, clustering_1) < (2, 3)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1, value2, value3, value4);
        Restriction slice2 = newMultiSlice(tableMetadata, 0, Bound.END, false, value2, value3);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);
        assertStartBound(get(bounds, 1), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2, value3, value4);
        assertEndBound(get(bounds, 1), false, value2, value3);

        // (clustering_0, clustering1, clustering_2, clustering_3) >= (1, 2, 3, 4) AND (clustering_0, clustering1, clustering_2, clustering_3) <= (4, 3, 2, 1)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2, value3, value4);
        slice2 = newMultiSlice(tableMetadata, 0, Bound.END, true, value4, value3, value2, value1);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);
        assertStartBound(get(bounds, 1), false, value1, value2);
        assertStartBound(get(bounds, 2), true, value4, value3, value2, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3, value4);
        assertEndBound(get(bounds, 1), false, value4, value3);
        assertEndBound(get(bounds, 2), true, value4, value3);
    }

    /**
     * Test multi-column slice restrictions with ascending, descending, ascending and descending columns
     * (e.g '(clustering_0, clustering1, clustering_3, clustering4) > (1, 2, 3, 4)')
     */
    @Test
    public void testBoundsAsClusteringWithMultiSliceRestrictionsWithAscendingDescendingColumnMix()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.DESC, Sort.ASC, Sort.DESC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);

        // (clustering_0, clustering1, clustering_2, clustering_3) > (1, 2, 3, 4)
        Restriction slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1, value2, value3, value4);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, slice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(4, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), true, value1, value2, value3);
        assertStartBound(get(bounds, 2), false, value1, value2, value3);
        assertStartBound(get(bounds, 3), false, value1);


        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(4, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);
        assertEndBound(get(bounds, 1), false, value1, value2, value3, value4);
        assertEndBound(get(bounds, 2), true, value1, value2);
        assertEmptyEnd(get(bounds, 3));

        // clustering_0 = 1 AND (clustering_1, clustering_2, clustering_3) > (2, 3, 4)
        Restriction eq = newSingleEq(tableMetadata, 0, value1);
        slice = newMultiSlice(tableMetadata, 1, Bound.START, false, value2, value3, value4);
        restrictions = restrictions(tableMetadata, slice, eq);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), true, value1, value2, value3);
        assertStartBound(get(bounds, 2), false, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);
        assertEndBound(get(bounds, 1), false, value1, value2, value3, value4);
        assertEndBound(get(bounds, 2), true, value1, value2);

        // (clustering_0, clustering1) >= (1, 2)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);
        assertEmptyEnd(get(bounds, 1));

        // (clustering_0, clustering1, clustering_2, clustering_3) >= (1, 2, 3, 4)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2, value3, value4);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(4, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), true, value1, value2, value3);
        assertStartBound(get(bounds, 2), false, value1, value2, value3);
        assertStartBound(get(bounds, 3), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(4, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);
        assertEndBound(get(bounds, 1), true, value1, value2, value3, value4);
        assertEndBound(get(bounds, 2), true, value1, value2);
        assertEmptyEnd(get(bounds, 3));

        // (clustering_0, clustering1, clustering_2, clustering_3) <= (1, 2, 3, 4)
        slice = newMultiSlice(tableMetadata, 0, Bound.END, true, value1, value2, value3, value4);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(4, bounds.size());
        assertEmptyStart(get(bounds, 0));
        assertStartBound(get(bounds, 1), true, value1, value2);
        assertStartBound(get(bounds, 2), true, value1, value2, value3, value4);
        assertStartBound(get(bounds, 3), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(4, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);
        assertEndBound(get(bounds, 1), false, value1, value2, value3);
        assertEndBound(get(bounds, 2), true, value1, value2, value3);
        assertEndBound(get(bounds, 3), true, value1);

        // (clustering_0, clustering1, clustering_2, clustering_3) < (1, 2, 3, 4)
        slice = newMultiSlice(tableMetadata, 0, Bound.END, false, value1, value2, value3, value4);
        restrictions = restrictions(tableMetadata, slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(4, bounds.size());
        assertEmptyStart(get(bounds, 0));
        assertStartBound(get(bounds, 1), true, value1, value2);
        assertStartBound(get(bounds, 2), false, value1, value2, value3, value4);
        assertStartBound(get(bounds, 3), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(4, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);
        assertEndBound(get(bounds, 1), false, value1, value2, value3);
        assertEndBound(get(bounds, 2), true, value1, value2, value3);
        assertEndBound(get(bounds, 3), true, value1);

        // (clustering_0, clustering1, clustering_2, clustering_3) > (1, 2, 3, 4) AND (clustering_0, clustering_1) < (2, 3)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, false, value1, value2, value3, value4);
        Restriction slice2 = newMultiSlice(tableMetadata, 0, Bound.END, false, value2, value3);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(5, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), true, value1, value2, value3);
        assertStartBound(get(bounds, 2), false, value1, value2, value3);
        assertStartBound(get(bounds, 3), false, value1);
        assertStartBound(get(bounds, 4), false, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(5, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);
        assertEndBound(get(bounds, 1), false, value1, value2, value3, value4);
        assertEndBound(get(bounds, 2), true, value1, value2);
        assertEndBound(get(bounds, 3), false, value2);
        assertEndBound(get(bounds, 4), true, value2);

        // (clustering_0, clustering1, clustering_2, clustering_3) >= (1, 2, 3, 4) AND (clustering_0, clustering1, clustering_2, clustering_3) <= (4, 3, 2, 1)
        slice = newMultiSlice(tableMetadata, 0, Bound.START, true, value1, value2, value3, value4);
        slice2 = newMultiSlice(tableMetadata, 0, Bound.END, true, value4, value3, value2, value1);
        restrictions = restrictions(tableMetadata, slice, slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(7, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), true, value1, value2, value3);
        assertStartBound(get(bounds, 2), false, value1, value2, value3);
        assertStartBound(get(bounds, 3), false, value1);
        assertStartBound(get(bounds, 4), true, value4, value3);
        assertStartBound(get(bounds, 5), true, value4, value3, value2, value1);
        assertStartBound(get(bounds, 6), false, value4, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(7, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);
        assertEndBound(get(bounds, 1), true, value1, value2, value3, value4);
        assertEndBound(get(bounds, 2), true, value1, value2);
        assertEndBound(get(bounds, 3), false, value4);
        assertEndBound(get(bounds, 4), false, value4, value3, value2);
        assertEndBound(get(bounds, 5), true, value4, value3, value2);
        assertEndBound(get(bounds, 6), true, value4);
    }

    /**
     * Test mixing single and multi equals restrictions (e.g. clustering_0 = 1 AND (clustering_1, clustering_2) = (2, 3))
     */
    @Test
    public void testBoundsAsClusteringWithSingleEqAndMultiEqRestrictions()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC, Sort.ASC, Sort.ASC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);

        // clustering_0 = 1 AND (clustering_1, clustering_2) = (2, 3)
        Restriction singleEq = newSingleEq(tableMetadata, 0, value1);
        Restriction multiEq = newMultiEq(tableMetadata, 1, value2, value3);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, singleEq, multiEq);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3);

        // clustering_0 = 1 AND clustering_1 = 2 AND (clustering_2, clustering_3) = (3, 4)
        singleEq = newSingleEq(tableMetadata, 0, value1);
        Restriction singleEq2 = newSingleEq(tableMetadata, 1, value2);
        multiEq = newMultiEq(tableMetadata, 2, value3, value4);
        restrictions = restrictions(tableMetadata, singleEq, singleEq2, multiEq);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3, value4);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3, value4);

        // (clustering_0, clustering_1) = (1, 2) AND clustering_2 = 3
        singleEq = newSingleEq(tableMetadata, 2, value3);
        multiEq = newMultiEq(tableMetadata, 0, value1, value2);
        restrictions = restrictions(tableMetadata, singleEq, multiEq);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3);

        // clustering_0 = 1 AND (clustering_1, clustering_2) = (2, 3) AND clustering_3 = 4
        singleEq = newSingleEq(tableMetadata, 0, value1);
        singleEq2 = newSingleEq(tableMetadata, 3, value4);
        multiEq = newMultiEq(tableMetadata, 1, value2, value3);
        restrictions = restrictions(tableMetadata, singleEq, multiEq, singleEq2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3, value4);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3, value4);
    }

    /**
     * Test clustering_0 = 1 AND (clustering_1, clustering_2) IN ((2, 3), (4, 5))
     */
    @Test
    public void testBoundsAsClusteringWithSingleEqAndMultiINRestrictions()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC, Sort.ASC, Sort.ASC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);
        ByteBuffer value5 = ByteBufferUtil.bytes(5);

        // clustering_0 = 1 AND (clustering_1, clustering_2) IN ((2, 3), (4, 5))
        Restriction singleEq = newSingleEq(tableMetadata, 0, value1);
        Restriction multiIN = newMultiIN(tableMetadata, 1, asList(value2, value3), asList(value4, value5));
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, singleEq, multiIN);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3);
        assertStartBound(get(bounds, 1), true, value1, value4, value5);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3);
        assertEndBound(get(bounds, 1), true, value1, value4, value5);

        // clustering_0 = 1 AND (clustering_1, clustering_2) IN ((2, 3))
        singleEq = newSingleEq(tableMetadata, 0, value1);
        multiIN = newMultiIN(tableMetadata, 1, asList(value2, value3));
        restrictions = restrictions(tableMetadata, multiIN, singleEq);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3);

        // clustering_0 = 1 AND clustering_1 = 5 AND (clustering_2, clustering_3) IN ((2, 3), (4, 5))
        singleEq = newSingleEq(tableMetadata, 0, value1);
        Restriction singleEq2 = newSingleEq(tableMetadata, 1, value5);
        multiIN = newMultiIN(tableMetadata, 2, asList(value2, value3), asList(value4, value5));
        restrictions = restrictions(tableMetadata, singleEq, multiIN, singleEq2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value5, value2, value3);
        assertStartBound(get(bounds, 1), true, value1, value5, value4, value5);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value5, value2, value3);
        assertEndBound(get(bounds, 1), true, value1, value5, value4, value5);
    }

    /**
     * Test mixing single equal restrictions with multi-column slice restrictions
     * (e.g. clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3))
     */
    @Test
    public void testBoundsAsClusteringWithSingleEqAndSliceRestrictions()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC, Sort.ASC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);
        ByteBuffer value5 = ByteBufferUtil.bytes(5);

        // clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3)
        Restriction singleEq = newSingleEq(tableMetadata, 0, value1);
        Restriction multiSlice = newMultiSlice(tableMetadata, 1, Bound.START, false, value2, value3);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, singleEq, multiSlice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1);

        // clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3) AND (clustering_1) < (4)
        singleEq = newSingleEq(tableMetadata, 0, value1);
        multiSlice = newMultiSlice(tableMetadata, 1, Bound.START, false, value2, value3);
        Restriction multiSlice2 = newMultiSlice(tableMetadata, 1, Bound.END, false, value4);
        restrictions = restrictions(tableMetadata, multiSlice2, singleEq, multiSlice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value4);

        // clustering_0 = 1 AND (clustering_1, clustering_2) => (2, 3) AND (clustering_1, clustering_2) <= (4, 5)
        singleEq = newSingleEq(tableMetadata, 0, value1);
        multiSlice = newMultiSlice(tableMetadata, 1, Bound.START, true, value2, value3);
        multiSlice2 = newMultiSlice(tableMetadata, 1, Bound.END, true, value4, value5);
        restrictions = restrictions(tableMetadata, multiSlice2, singleEq, multiSlice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value4, value5);
    }

    /**
     * Test mixing multi equal restrictions with single-column slice restrictions
     * (e.g. clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3))
     */
    @Test
    public void testBoundsAsClusteringWithMultiEqAndSingleSliceRestrictions()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC, Sort.ASC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);

        // (clustering_0, clustering_1) = (1, 2) AND clustering_2 > 3
        Restriction multiEq = newMultiEq(tableMetadata, 0, value1, value2);
        Restriction singleSlice = newSingleSlice(tableMetadata, 2, Bound.START, false, value3);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, multiEq, singleSlice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);
    }

    @Test
    public void testBoundsAsClusteringWithSeveralMultiColumnRestrictions()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC, Sort.ASC, Sort.ASC);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);
        ByteBuffer value5 = ByteBufferUtil.bytes(5);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) > (3, 4)
        Restriction multiEq = newMultiEq(tableMetadata, 0, value1, value2);
        Restriction multiSlice = newMultiSlice(tableMetadata, 2, Bound.START, false, value3, value4);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, multiEq, multiSlice);

        SortedSet<ClusteringBound<?>> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1, value2, value3, value4);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) IN ((3, 4), (4, 5))
        multiEq = newMultiEq(tableMetadata, 0, value1, value2);
        Restriction multiIN = newMultiIN(tableMetadata, 2, asList(value3, value4), asList(value4, value5));
        restrictions = restrictions(tableMetadata, multiEq, multiIN);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3, value4);
        assertStartBound(get(bounds, 1), true, value1, value2, value4, value5);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3, value4);
        assertEndBound(get(bounds, 1), true, value1, value2, value4, value5);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) = (3, 4)
        multiEq = newMultiEq(tableMetadata, 0, value1, value2);
        Restriction multiEq2 = newMultiEq(tableMetadata, 2, value3, value4);
        restrictions = restrictions(tableMetadata, multiEq, multiEq2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3, value4);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3, value4);
    }

    /**
     * Asserts that the specified <code>Bound</code> is an empty start.
     *
     * @param bound the bound to check
     */
    private static void assertEmptyStart(ClusteringBound<?> bound)
    {
        assertTrue(bound.isBottom());
    }

    /**
     * Asserts that the specified <code>Bound</code> is an empty end.
     *
     * @param bound the bound to check
     */
    private static void assertEmptyEnd(ClusteringBound<?> bound)
    {
        assertTrue(bound.isTop());
    }

    /**
     * Asserts that the specified <code>ClusteringBound</code> is a start with the specified elements.
     *
     * @param bound the bound to check
     * @param isInclusive if the bound is expected to be inclusive
     * @param elements the expected elements of the clustering
     */
    private static void assertStartBound(ClusteringBound<?> bound, boolean isInclusive, ByteBuffer... elements)
    {
        assertBound(bound, true, isInclusive, elements);
    }

    /**
     * Asserts that the specified <code>ClusteringBound</code> is a end with the specified elements.
     *
     * @param bound the bound to check
     * @param isInclusive if the bound is expected to be inclusive
     * @param elements the expected elements of the clustering
     */
    private static void assertEndBound(ClusteringBound<?> bound, boolean isInclusive, ByteBuffer... elements)
    {
        assertBound(bound, false, isInclusive, elements);
    }

    private static void assertBound(ClusteringBound<?> bound, boolean isStart, boolean isInclusive, ByteBuffer... elements)
    {
        assertEquals("the bound size is not the expected one:", elements.length, bound.size());
        assertEquals("the bound should be a " + (isStart ? "start" : "end") + " but is a " + (bound.isStart() ? "start" : "end"), isStart, bound.isStart());
        assertEquals("the bound inclusiveness is not the expected one", isInclusive, bound.isInclusive());
        for (int i = 0, m = elements.length; i < m; i++)
        {
            ByteBuffer element = elements[i];
            assertEquals(String.format("the element %s of the bound is not the expected one: expected %s but was %s",
                                       i,
                                       ByteBufferUtil.toInt(element),
                                       ByteBufferUtil.toInt(bound.bufferAt(i))), bound.bufferAt(i), element);
        }
    }

    private static TableMetadata newTableMetadata(Sort... sorts)
    {
        List<AbstractType<?>> types = new ArrayList<>();

        for (Sort sort : sorts)
            types.add(sort == Sort.ASC ? Int32Type.instance : ReversedType.getInstance(Int32Type.instance));

        TableMetadata.Builder builder =
            TableMetadata.builder("keyspace", "test")
                         .addPartitionKeyColumn("partition_key", Int32Type.instance);

        for (int i = 0; i < sorts.length; i++)
            builder.addClusteringColumn("clustering_" + i, types.get(i));

        return builder.build();
    }

    /**
     * Creates a new <code>SingleColumnRestriction.EQ</code> instance for the specified clustering column.
     *
     * @param tableMetadata the column family meta data
     * @param index the clustering column index
     * @param value the equality value
     * @return a new <code>SingleColumnRestriction.EQ</code> instance for the specified clustering column
     */
    private static Restriction newSingleEq(TableMetadata tableMetadata, int index, ByteBuffer value)
    {
        ColumnMetadata columnDef = getClusteringColumnDefinition(tableMetadata, index);
        return new SingleColumnRestriction.EQRestriction(columnDef, toTerm(value));
    }

    /**
     * Creates a new <code>MultiColumnRestriction.EQ</code> instance for the specified clustering column.
     *
     * @param tableMetadata the column family meta data
     * @param firstIndex the clustering column index
     * @param values the equality value
     * @return a new <code>MultiColumnRestriction.EQ</code> instance for the specified clustering column
     */
    private static Restriction newMultiEq(TableMetadata tableMetadata, int firstIndex, ByteBuffer... values)
    {
        List<ColumnMetadata> columnMetadatas = new ArrayList<>();
        for (int i = 0; i < values.length; i++)
        {
            columnMetadatas.add(getClusteringColumnDefinition(tableMetadata, firstIndex + i));
        }
        return new MultiColumnRestriction.EQRestriction(columnMetadatas, toMultiItemTerminal(values));
    }

    /**
     * Creates a new <code>MultiColumnRestriction.IN</code> instance for the specified clustering column.
     *
     * @param tableMetadata the column family meta data
     * @param firstIndex the index of the first clustering column
     * @param values the in values
     * @return a new <code>MultiColumnRestriction.IN</code> instance for the specified clustering column
     */
    @SafeVarargs
    private static Restriction newMultiIN(TableMetadata tableMetadata, int firstIndex, List<ByteBuffer>... values)
    {
        List<ColumnMetadata> columnMetadatas = new ArrayList<>();
        List<Term> terms = new ArrayList<>();
        for (int i = 0; i < values.length; i++)
        {
            columnMetadatas.add(getClusteringColumnDefinition(tableMetadata, firstIndex + i));
            terms.add(toMultiItemTerminal(values[i].toArray(new ByteBuffer[0])));
        }
        return new MultiColumnRestriction.InRestrictionWithValues(columnMetadatas, terms);
    }

    /**
     * Creates a new <code>SingleColumnRestriction.IN</code> instance for the specified clustering column.
     *
     * @param tableMetadata the column family meta data
     * @param index the clustering column index
     * @param values the in values
     * @return a new <code>SingleColumnRestriction.IN</code> instance for the specified clustering column
     */
    private static Restriction newSingleIN(TableMetadata tableMetadata, int index, ByteBuffer... values)
    {
        ColumnMetadata columnDef = getClusteringColumnDefinition(tableMetadata, index);
        return new SingleColumnRestriction.InRestrictionWithValues(columnDef, toTerms(values));
    }

    /**
     * Returns the clustering <code>ColumnMetadata</code> for the specified position.
     *
     * @param tableMetadata the column family meta data
     * @param index the clustering column index
     * @return the clustering <code>ColumnMetadata</code> for the specified position.
     */
    private static ColumnMetadata getClusteringColumnDefinition(TableMetadata tableMetadata, int index)
    {
        return tableMetadata.clusteringColumns().get(index);
    }

    /**
     * Creates a new <code>SingleColumnRestriction.Slice</code> instance for the specified clustering column.
     *
     * @param tableMetadata the column family meta data
     * @param index the clustering column index
     * @param bound the slice bound
     * @param inclusive <code>true</code> if the bound is inclusive
     * @param value the bound value
     * @return a new <code>SingleColumnRestriction.Slice</code> instance for the specified clustering column
     */
    private static Restriction newSingleSlice(TableMetadata tableMetadata, int index, Bound bound, boolean inclusive, ByteBuffer value)
    {
        ColumnMetadata columnDef = getClusteringColumnDefinition(tableMetadata, index);
        return new SingleColumnRestriction.SliceRestriction(columnDef, bound, inclusive, toTerm(value));
    }

    /**
     * Creates a new <code>SingleColumnRestriction.Slice</code> instance for the specified clustering column.
     *
     * @param tableMetadata the column family meta data
     * @param firstIndex the clustering column index
     * @param bound the slice bound
     * @param inclusive <code>true</code> if the bound is inclusive
     * @param values the bound value
     * @return a new <code>SingleColumnRestriction.Slice</code> instance for the specified clustering column
     */
    private static Restriction newMultiSlice(TableMetadata tableMetadata, int firstIndex, Bound bound, boolean inclusive, ByteBuffer... values)
    {
        List<ColumnMetadata> columnMetadatas = new ArrayList<>();
        for (int i = 0; i < values.length; i++)
        {
            columnMetadatas.add(getClusteringColumnDefinition(tableMetadata, i + firstIndex));
        }
        return new MultiColumnRestriction.SliceRestriction(columnMetadatas, bound, inclusive, toMultiItemTerminal(values));
    }

    /**
     * Converts the specified values into a <code>MultiItemTerminal</code>.
     *
     * @param values the values to convert.
     * @return the term corresponding to the specified values.
     */
    private static MultiItemTerminal toMultiItemTerminal(ByteBuffer... values)
    {
        return new Tuples.Value(values);
    }

    /**
     * Converts the specified value into a term.
     *
     * @param value the value to convert.
     * @return the term corresponding to the specified value.
     */
    private static Term toTerm(ByteBuffer value)
    {
        return new Constants.Value(value);
    }

    /**
     * Converts the specified values into a <code>List</code> of terms.
     *
     * @param values the values to convert.
     * @return a <code>List</code> of terms corresponding to the specified values.
     */
    private static List<Term> toTerms(ByteBuffer... values)
    {
        List<Term> terms = new ArrayList<>();
        for (ByteBuffer value : values)
            terms.add(toTerm(value));
        return terms;
    }

    private static <T> T get(SortedSet<T> set, int i)
    {
        return Iterables.get(set, i);
    }
    
    private static ClusteringColumnRestrictions restrictions(TableMetadata table, Restriction... restrictions)
    {
        ClusteringColumnRestrictions clusteringColumnRestrictions = new ClusteringColumnRestrictions(table, false);
        for (Restriction restriction : restrictions)
            clusteringColumnRestrictions = clusteringColumnRestrictions.mergeWith(restriction, null);
        return clusteringColumnRestrictions;
    }

    private enum Sort
    {
        ASC,
        DESC;
    }
}
