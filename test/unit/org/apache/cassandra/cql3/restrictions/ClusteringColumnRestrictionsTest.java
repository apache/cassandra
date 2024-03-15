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

import org.apache.cassandra.cql3.terms.Constants;
import org.apache.cassandra.cql3.terms.MultiElements;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.cql3.terms.Terms;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;

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

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        assertEquals(slices.get(0), Slice.ALL);
    }

    /**
     * Test 'clustering_0 = 1' with only one clustering column
     */
    @Test
    public void testBoundsAsClusteringWithOneEqRestrictionsAndOneClusteringColumn()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC);

        ByteBuffer clustering_0 = ByteBufferUtil.bytes(1);
        Restriction eq = newSingleRestriction(tableMetadata, 0, Operator.EQ, clustering_0);

        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, eq);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        Slice slice = slices.get(0);
        assertStartBound(slice, true, clustering_0);
        assertEndBound(slice, true, clustering_0);
    }

    /**
     * Test 'clustering_1 = 1' with 2 clustering columns
     */
    @Test
    public void testBoundsAsClusteringWithOneEqRestrictionsAndTwoClusteringColumns()
    {
        TableMetadata tableMetadata = newTableMetadata(Sort.ASC, Sort.ASC);

        ByteBuffer clustering_0 = ByteBufferUtil.bytes(1);
        Restriction eq = newSingleRestriction(tableMetadata, 0, Operator.EQ, clustering_0);

        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, eq);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        Slice slice = slices.get(0);
        assertStartBound(slice, true, clustering_0);
        assertEndBound(slice, true, clustering_0);
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

        Restriction in = newSingleRestriction(tableMetadata, 0, Operator.IN, value1, value2, value3);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, in);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(3, slices.size());
        Slice slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, true, value1);
        slice = slices.get(1);
        assertStartBound(slice, true, value2);
        assertEndBound(slice, true, value2);
        slice = slices.get(2);
        assertStartBound(slice, true, value3);
        assertEndBound(slice, true, value3);

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

        Restriction restriction = newSingleRestriction(tableMetadata, 0, Operator.GT, value1);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, restriction);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        Slice slice = slices.get(0);
        assertStartBound(slice, false, value1);
        assertEmptyEnd(slice);

        restriction = newSingleRestriction(tableMetadata, 0, Operator.GTE, value1);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEmptyEnd(slice);

        restriction = newSingleRestriction(tableMetadata, 0, Operator.LTE, value1);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, true, value1);

        restriction = newSingleRestriction(tableMetadata, 0, Operator.LT, value1);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1);

        restriction = newSingleRestriction(tableMetadata, 0, Operator.GT, value1);
        Restriction restriction2 = newSingleRestriction(tableMetadata, 0, Operator.LT, value2);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, false, value1);
        assertEndBound(slice, false, value2);

        restriction = newSingleRestriction(tableMetadata, 0, Operator.GTE, value1);
        restriction2 = newSingleRestriction(tableMetadata, 0, Operator.LTE, value2);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, true, value2);
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

        Restriction restriction = newSingleRestriction(tableMetadata, 0, Operator.GT, value1);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, restriction);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        Slice slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1);

        restriction = newSingleRestriction(tableMetadata, 0, Operator.GTE, value1);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, true, value1);

        restriction = newSingleRestriction(tableMetadata, 0, Operator.LTE, value1);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEmptyEnd(slice);

        restriction = newSingleRestriction(tableMetadata, 0, Operator.LT, value1);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, false, value1);
        assertEmptyEnd(slice);

        restriction = newSingleRestriction(tableMetadata, 0, Operator.GT, value1);
        Restriction restriction2 = newSingleRestriction(tableMetadata, 0, Operator.LT, value2);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, false, value2);
        assertEndBound(slice, false, value1);

        restriction = newSingleRestriction(tableMetadata, 0, Operator.GTE, value1);
        restriction2 = newSingleRestriction(tableMetadata, 0, Operator.LTE, value2);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value2);
        assertEndBound(slice, true, value1);
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
        Restriction eq = newSingleRestriction(tableMetadata, 0, Operator.EQ, value1);
        Restriction in = newSingleRestriction(tableMetadata, 1, Operator.IN, value1, value2, value3);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, eq, in);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(3, slices.size());
        Slice slice = slices.get(0);
        assertStartBound(slice, true, value1, value1);
        assertEndBound(slice, true, value1, value1);
        slice = slices.get(1);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, true, value1, value2);
        slice = slices.get(2);
        assertStartBound(slice, true, value1, value3);
        assertEndBound(slice, true, value1, value3);
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

        Restriction eq = newSingleRestriction(tableMetadata, 0, Operator.EQ, value3);

        Restriction restriction = newSingleRestriction(tableMetadata, 1, Operator.GT, value1);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, eq, restriction);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        Slice slice = slices.get(0);
        assertStartBound(slice, false, value3, value1);
        assertEndBound(slice, true, value3);

        restriction = newSingleRestriction(tableMetadata, 1, Operator.GTE, value1);
        restrictions = restrictions(tableMetadata, eq, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value3, value1);
        assertEndBound(slice, true, value3);

        restriction = newSingleRestriction(tableMetadata, 1, Operator.LTE, value1);
        restrictions =  restrictions(tableMetadata, eq, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value3);
        assertEndBound(slice, true, value3, value1);

        restriction = newSingleRestriction(tableMetadata, 1, Operator.LT, value1);
        restrictions =  restrictions(tableMetadata, eq, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value3);
        assertEndBound(slice, false, value3, value1);

        restriction = newSingleRestriction(tableMetadata, 1, Operator.GT, value1);
        Restriction restriction2 = newSingleRestriction(tableMetadata, 1, Operator.LT, value2);
        restrictions =  restrictions(tableMetadata, eq, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, false, value3, value1);
        assertEndBound(slice, false, value3, value2);

        restriction = newSingleRestriction(tableMetadata, 1, Operator.GTE, value1);
        restriction2 = newSingleRestriction(tableMetadata, 1, Operator.LTE, value2);
        restrictions =  restrictions(tableMetadata, eq, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value3, value1);
        assertEndBound(slice, true, value3, value2);
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

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        Slice slice = slices.get(0);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, true, value1, value2);
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

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        Slice slice = slices.get(0);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, true, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, true, value2, value3);
        assertEndBound(slice, true, value2, value3);
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

        Restriction restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, restriction);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        Slice slice = slices.get(0);
        assertStartBound(slice, false, value1);
        assertEmptyEnd(slice);

        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEmptyEnd(slice);

        restriction = newMultiSlice(tableMetadata, 0, Operator.LTE, value1);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, true, value1);

        restriction = newMultiSlice(tableMetadata, 0, Operator.LT, value1);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1);

        restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1);
        Restriction restriction2 = newMultiSlice(tableMetadata, 0, Operator.LT, value2);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, false, value1);
        assertEndBound(slice, false, value2);

        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1);
        restriction2 = newMultiSlice(tableMetadata, 0, Operator.LTE, value2);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, true, value2);
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

        Restriction restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1);
        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, restriction);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        Slice slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1);

        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, true, value1);

        restriction = newMultiSlice(tableMetadata, 0, Operator.LTE, value1);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEmptyEnd(slice);

        restriction = newMultiSlice(tableMetadata, 0, Operator.LT, value1);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, false, value1);
        assertEmptyEnd(slice);

        restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1);
        Restriction restriction2 = newMultiSlice(tableMetadata, 0, Operator.LT, value2);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, false, value2);
        assertEndBound(slice, false, value1);

        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1);
        restriction2 = newMultiSlice(tableMetadata, 0, Operator.LTE, value2);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value2);
        assertEndBound(slice, true, value1);
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
        Restriction restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1, value2);
        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, restriction);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        Slice slice = slices.get(0);
        assertStartBound(slice, false, value1, value2);
        assertEmptyEnd(slice);

        // (clustering_0, clustering1) >= (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2);
        assertEmptyEnd(slice);

        // (clustering_0, clustering1) <= (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.LTE, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, true, value1, value2);

        // (clustering_0, clustering1) < (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.LT, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1, value2);

        // (clustering_0, clustering1) > (1, 2) AND (clustering_0) < (2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1, value2);
        Restriction restriction2 = newMultiSlice(tableMetadata, 0, Operator.LT, value2);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, false, value1, value2);
        assertEndBound(slice, false, value2);

        // (clustering_0, clustering1) >= (1, 2) AND (clustering_0, clustering1) <= (2, 1)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2);
        restriction2 = newMultiSlice(tableMetadata, 0, Operator.LTE, value2, value1);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, true, value2, value1);
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
        Restriction restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1, value2);
        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, restriction);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        Slice slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1, value2);

        // (clustering_0, clustering1) >= (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, true, value1, value2);

        // (clustering_0, clustering1) <= (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.LTE, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2);
        assertEmptyEnd(slice);

        // (clustering_0, clustering1) < (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.LT, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, false, value1, value2);
        assertEmptyEnd(slice);

        // (clustering_0, clustering1) > (1, 2) AND (clustering_0) < (2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1, value2);
        Restriction restriction2 = newMultiSlice(tableMetadata, 0, Operator.LT, value2);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, false, value2);
        assertEndBound(slice, false, value1, value2);

        // (clustering_0, clustering1) >= (1, 2) AND (clustering_0, clustering1) <= (2, 1)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2);
        restriction2 = newMultiSlice(tableMetadata, 0, Operator.LTE, value2, value1);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());
        slice = slices.get(0);
        assertStartBound(slice, true, value2, value1);
        assertEndBound(slice, true, value1, value2);
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
        Restriction restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1, value2);
        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, restriction);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        Slice slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1);

        slice = slices.get(1);
        assertEndBound(slice, true, value1);
        assertStartBound(slice, false, value1, value2);

        // (clustering_0, clustering1) >= (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1);

        slice = slices.get(1);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, true, value1);

        // (clustering_0, clustering1) <= (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.LTE, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, true, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, false, value1);
        assertEmptyEnd(slice);

        // (clustering_0, clustering1) < (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.LT, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, false, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, false, value1);
        assertEmptyEnd(slice);

        // (clustering_0, clustering1) > (1, 2) AND (clustering_0) < (2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1, value2);
        Restriction restriction2 = newMultiSlice(tableMetadata, 0, Operator.LT, value2);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, false, value2);
        assertEndBound(slice, false, value1);

        slice = slices.get(1);
        assertEndBound(slice, true, value1);
        assertStartBound(slice, false, value1, value2);

        // (clustering_0) > (1) AND (clustering_0, clustering1) < (2, 1)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1);
        restriction2 = newMultiSlice(tableMetadata, 0, Operator.LT, value2, value1);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value2);
        assertEndBound(slice, false, value2, value1);

        slice = slices.get(1);
        assertStartBound(slice, false, value2);
        assertEndBound(slice, false, value1);

        // (clustering_0, clustering1) >= (1, 2) AND (clustering_0, clustering1) <= (2, 1)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2);
        restriction2 = newMultiSlice(tableMetadata, 0, Operator.LTE, value2, value1);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(3, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value2);
        assertEndBound(slice, true, value2, value1);

        slice = slices.get(1);
        assertEndBound(slice, false, value1);
        assertStartBound(slice, false, value2);

        slice = slices.get(2);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, true, value1);
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
        Restriction restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1, value2);
        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, restriction);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        Slice slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, false, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, false, value1);
        assertEmptyEnd(slice);

        // (clustering_0, clustering1) >= (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, true, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, false, value1);
        assertEmptyEnd(slice);

        // (clustering_0, clustering1) <= (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.LTE, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1);

        slice = slices.get(1);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, true, value1);

        // (clustering_0, clustering1) < (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.LT, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1);

        slice = slices.get(1);
        assertStartBound(slice, false, value1, value2);
        assertEndBound(slice, true, value1);

        // (clustering_0, clustering1) > (1, 2) AND (clustering_0) < (2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1, value2);
        Restriction restriction2 = newMultiSlice(tableMetadata, 0, Operator.LT, value2);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, false, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, false, value1);
        assertEndBound(slice, false, value2);

        // (clustering_0, clustering1) >= (1, 2) AND (clustering_0, clustering1) <= (2, 1)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2);
        restriction2 = newMultiSlice(tableMetadata, 0, Operator.LTE, value2, value1);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(3, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, true, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, false, value1);
        assertEndBound(slice, false, value2);

        slice = slices.get(2);
        assertStartBound(slice, true, value2, value1);
        assertEndBound(slice, true, value2);
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
        Restriction restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1, value2, value3, value4);
        ClusteringColumnRestrictions restrictions =  restrictions(tableMetadata, restriction);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        Slice slice = slices.get(0);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, false, value1, value2, value3, value4);

        slice = slices.get(1);
        assertStartBound(slice, false, value1, value2);
        assertEmptyEnd(slice);

        // clustering_0 = 1 AND (clustering_1, clustering_2, clustering_3) > (2, 3, 4)
        Restriction eq = newSingleRestriction(tableMetadata, 0, Operator.EQ, value1);
        restriction = newMultiSlice(tableMetadata, 1, Operator.GT, value2, value3, value4);
        restrictions = restrictions(tableMetadata, restriction, eq);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, false, value1, value2, value3, value4);

        slice = slices.get(1);
        assertStartBound(slice, false, value1, value2);
        assertEndBound(slice, true, value1);

        // clustering_0 IN (1, 2) AND (clustering_1, clustering_2, clustering_3) > (2, 3, 4)
        Restriction in = newSingleRestriction(tableMetadata, 0, Operator.IN, value1, value2);
        restriction = newMultiSlice(tableMetadata, 1, Operator.GT, value2, value3, value4);
        restrictions = restrictions(tableMetadata, restriction, in);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(4, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, false, value1, value2, value3, value4);

        slice = slices.get(1);
        assertStartBound(slice, false, value1, value2);
        assertEndBound(slice, true, value1);

        slice = slices.get(2);
        assertStartBound(slice, true, value2, value2);
        assertEndBound(slice, false, value2, value2, value3, value4);

        slice = slices.get(3);
        assertStartBound(slice, false, value2, value2);
        assertEndBound(slice, true, value2);

        // (clustering_0, clustering1) >= (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2);
        assertEmptyEnd(slice);

        // (clustering_0, clustering1, clustering_2, clustering_3) >= (1, 2, 3, 4)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2, value3, value4);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, true, value1, value2, value3, value4);
        slice = slices.get(1);
        assertStartBound(slice, false, value1, value2);
        assertEmptyEnd(slice);

        // (clustering_0, clustering1, clustering_2, clustering_3) <= (1, 2, 3, 4)
        restriction = newMultiSlice(tableMetadata, 0, Operator.LTE, value1, value2, value3, value4);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, true, value1, value2, value3, value4);
        assertEndBound(slice, true, value1, value2);

        // (clustering_0, clustering1, clustering_2, clustering_3) < (1, 2, 3, 4)
        restriction = newMultiSlice(tableMetadata, 0, Operator.LT, value1, value2, value3, value4);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, false, value1, value2, value3, value4);
        assertEndBound(slice, true, value1, value2);

        // (clustering_0, clustering1, clustering_2, clustering_3) > (1, 2, 3, 4) AND (clustering_0, clustering_1) < (2, 3)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1, value2, value3, value4);
        Restriction restriction2 = newMultiSlice(tableMetadata, 0, Operator.LT, value2, value3);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, false, value1, value2, value3, value4);

        slice = slices.get(1);
        assertStartBound(slice, false, value1, value2);
        assertEndBound(slice, false, value2, value3);

        // (clustering_0, clustering1, clustering_2, clustering_3) >= (1, 2, 3, 4) AND (clustering_0, clustering1, clustering_2, clustering_3) <= (4, 3, 2, 1)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2, value3, value4);
        restriction2 = newMultiSlice(tableMetadata, 0, Operator.LTE, value4, value3, value2, value1);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(3, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, true, value1, value2, value3, value4);

        slice = slices.get(1);
        assertStartBound(slice, false, value1, value2);
        assertEndBound(slice, false, value4, value3);

        slice = slices.get(2);
        assertStartBound(slice, true, value4, value3, value2, value1);
        assertEndBound(slice, true, value4, value3);
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
        Restriction restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1, value2, value3, value4);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, restriction);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(4, slices.size());

        Slice slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, false, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, true, value1, value2, value3);
        assertEndBound(slice, false, value1, value2, value3, value4);

        slice = slices.get(2);
        assertStartBound(slice, false, value1, value2, value3);
        assertEndBound(slice, true, value1, value2);

        slice = slices.get(3);
        assertStartBound(slice, false, value1);
        assertEmptyEnd(slice);

        // clustering_0 = 1 AND (clustering_1, clustering_2, clustering_3) > (2, 3, 4)
        Restriction eq = newSingleRestriction(tableMetadata, 0, Operator.EQ, value1);
        restriction = newMultiSlice(tableMetadata, 1, Operator.GT, value2, value3, value4);
        restrictions = restrictions(tableMetadata, restriction, eq);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(3, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, false, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, true, value1, value2, value3);
        assertEndBound(slice, false, value1, value2, value3, value4);

        slice = slices.get(2);
        assertStartBound(slice, false, value1, value2, value3);
        assertEndBound(slice, true, value1, value2);

        // (clustering_0, clustering1) >= (1, 2)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, true, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, false, value1);
        assertEmptyEnd(slice);

        // (clustering_0, clustering1, clustering_2, clustering_3) >= (1, 2, 3, 4)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2, value3, value4);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(4, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, false, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, true, value1, value2, value3);
        assertEndBound(slice, true, value1, value2, value3, value4);

        slice = slices.get(2);
        assertStartBound(slice, false, value1, value2, value3);
        assertEndBound(slice, true, value1, value2);

        slice = slices.get(3);
        assertStartBound(slice, false, value1);
        assertEmptyEnd(slice);

        // (clustering_0, clustering1, clustering_2, clustering_3) <= (1, 2, 3, 4)
        restriction = newMultiSlice(tableMetadata, 0, Operator.LTE, value1, value2, value3, value4);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(4, slices.size());

        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1);

        slice = slices.get(1);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, false, value1, value2, value3);

        slice = slices.get(2);
        assertStartBound(slice, true, value1, value2, value3, value4);
        assertEndBound(slice, true, value1, value2, value3);

        slice = slices.get(3);
        assertStartBound(slice, false, value1, value2);
        assertEndBound(slice, true, value1);

        // (clustering_0, clustering1, clustering_2, clustering_3) < (1, 2, 3, 4)
        restriction = newMultiSlice(tableMetadata, 0, Operator.LT, value1, value2, value3, value4);
        restrictions = restrictions(tableMetadata, restriction);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(4, slices.size());

        slice = slices.get(0);
        assertEmptyStart(slice);
        assertEndBound(slice, false, value1);

        slice = slices.get(1);
        assertStartBound(slice, true, value1, value2);
        assertEndBound(slice, false, value1, value2, value3);

        slice = slices.get(2);
        assertStartBound(slice, false, value1, value2, value3, value4);
        assertEndBound(slice, true, value1, value2, value3);

        slice = slices.get(3);
        assertStartBound(slice, false, value1, value2);
        assertEndBound(slice, true, value1);

        // (clustering_0, clustering1, clustering_2, clustering_3) > (1, 2, 3, 4) AND (clustering_0, clustering_1) < (2, 3)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GT, value1, value2, value3, value4);
        Restriction restriction2 = newMultiSlice(tableMetadata, 0, Operator.LT, value2, value3);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(5, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, false, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, true, value1, value2, value3);
        assertEndBound(slice, false, value1, value2, value3, value4);

        slice = slices.get(2);
        assertStartBound(slice, false, value1, value2, value3);
        assertEndBound(slice, true, value1, value2);

        slice = slices.get(3);
        assertStartBound(slice, false, value1);
        assertEndBound(slice, false, value2);

        slice = slices.get(4);
        assertStartBound(slice, false, value2, value3);
        assertEndBound(slice, true, value2);

        // (clustering_0, clustering1, clustering_2, clustering_3) >= (1, 2, 3, 4) AND (clustering_0, clustering1, clustering_2, clustering_3) <= (4, 3, 2, 1)
        restriction = newMultiSlice(tableMetadata, 0, Operator.GTE, value1, value2, value3, value4);
        restriction2 = newMultiSlice(tableMetadata, 0, Operator.LTE, value4, value3, value2, value1);
        restrictions = restrictions(tableMetadata, restriction, restriction2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(7, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1);
        assertEndBound(slice, false, value1, value2);

        slice = slices.get(1);
        assertStartBound(slice, true, value1, value2, value3);
        assertEndBound(slice, true, value1, value2, value3, value4);

        slice = slices.get(2);
        assertStartBound(slice, false, value1, value2, value3);
        assertEndBound(slice, true, value1, value2);

        slice = slices.get(3);
        assertStartBound(slice, false, value1);
        assertEndBound(slice, false, value4);

        slice = slices.get(4);
        assertStartBound(slice, true, value4, value3);
        assertEndBound(slice, false, value4, value3, value2);

        slice = slices.get(5);
        assertStartBound(slice, true, value4, value3, value2, value1);
        assertEndBound(slice, true, value4, value3, value2);

        slice = slices.get(6);
        assertStartBound(slice, false, value4, value3);
        assertEndBound(slice, true, value4);
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
        Restriction singleEq = newSingleRestriction(tableMetadata, 0, Operator.EQ, value1);
        Restriction multiEq = newMultiEq(tableMetadata, 1, value2, value3);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, singleEq, multiEq);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());

        Slice slice = slices.get(0);
        assertStartBound(slice, true, value1, value2, value3);
        assertEndBound(slice, true, value1, value2, value3);

        // clustering_0 = 1 AND clustering_1 = 2 AND (clustering_2, clustering_3) = (3, 4)
        singleEq = newSingleRestriction(tableMetadata, 0, Operator.EQ, value1);
        Restriction singleEq2 = newSingleRestriction(tableMetadata, 1, Operator.EQ, value2);
        multiEq = newMultiEq(tableMetadata, 2, value3, value4);
        restrictions = restrictions(tableMetadata, singleEq, singleEq2, multiEq);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2, value3, value4);
        assertEndBound(slice, true, value1, value2, value3, value4);

        // (clustering_0, clustering_1) = (1, 2) AND clustering_2 = 3
        singleEq = newSingleRestriction(tableMetadata, 2, Operator.EQ, value3);
        multiEq = newMultiEq(tableMetadata, 0, value1, value2);
        restrictions = restrictions(tableMetadata, singleEq, multiEq);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2, value3);
        assertEndBound(slice, true, value1, value2, value3);

        // clustering_0 = 1 AND (clustering_1, clustering_2) = (2, 3) AND clustering_3 = 4
        singleEq = newSingleRestriction(tableMetadata, 0, Operator.EQ, value1);
        singleEq2 = newSingleRestriction(tableMetadata, 3, Operator.EQ, value4);
        multiEq = newMultiEq(tableMetadata, 1, value2, value3);
        restrictions = restrictions(tableMetadata, singleEq, multiEq, singleEq2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2, value3, value4);
        assertEndBound(slice, true, value1, value2, value3, value4);
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
        Restriction singleEq = newSingleRestriction(tableMetadata, 0, Operator.EQ, value1);
        Restriction multiIN = newMultiIN(tableMetadata, 1, asList(value2, value3), asList(value4, value5));
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, singleEq, multiIN);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        Slice slice = slices.get(0);
        assertStartBound(slice, true, value1, value2, value3);
        assertEndBound(slice, true, value1, value2, value3);

        slice = slices.get(1);
        assertStartBound(slice, true, value1, value4, value5);
        assertEndBound(slice, true, value1, value4, value5);

        // clustering_0 = 1 AND (clustering_1, clustering_2) IN ((2, 3))
        singleEq = newSingleRestriction(tableMetadata, 0, Operator.EQ, value1);
        multiIN = newMultiIN(tableMetadata, 1, asList(value2, value3));
        restrictions = restrictions(tableMetadata, multiIN, singleEq);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());

        slice = slices.get(0);

        assertStartBound(slice, true, value1, value2, value3);
        assertEndBound(slice, true, value1, value2, value3);

        // clustering_0 = 1 AND clustering_1 = 5 AND (clustering_2, clustering_3) IN ((2, 3), (4, 5))
        singleEq = newSingleRestriction(tableMetadata, 0, Operator.EQ, value1);
        Restriction singleEq2 = newSingleRestriction(tableMetadata, 1, Operator.EQ, value5);
        multiIN = newMultiIN(tableMetadata, 2, asList(value2, value3), asList(value4, value5));
        restrictions = restrictions(tableMetadata, singleEq, multiIN, singleEq2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1, value5, value2, value3);
        assertEndBound(slice, true, value1, value5, value2, value3);

        slice = slices.get(1);
        assertStartBound(slice, true, value1, value5, value4, value5);
        assertEndBound(slice, true, value1, value5, value4, value5);
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
        Restriction singleEq = newSingleRestriction(tableMetadata, 0, Operator.EQ, value1);
        Restriction multiSlice = newMultiSlice(tableMetadata, 1, Operator.GT, value2, value3);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, singleEq, multiSlice);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());

        Slice slice = slices.get(0);
        assertStartBound(slice, false, value1, value2, value3);
        assertEndBound(slice, true, value1);

        // clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3) AND (clustering_1) < (4)
        singleEq = newSingleRestriction(tableMetadata, 0, Operator.EQ, value1);
        multiSlice = newMultiSlice(tableMetadata, 1, Operator.GT, value2, value3);
        Restriction multiSlice2 = newMultiSlice(tableMetadata, 1, Operator.LT, value4);
        restrictions = restrictions(tableMetadata, multiSlice2, singleEq, multiSlice);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, false, value1, value2, value3);
        assertEndBound(slice, false, value1, value4);

        // clustering_0 = 1 AND (clustering_1, clustering_2) => (2, 3) AND (clustering_1, clustering_2) <= (4, 5)
        singleEq = newSingleRestriction(tableMetadata, 0, Operator.EQ, value1);
        multiSlice = newMultiSlice(tableMetadata, 1, Operator.GTE, value2, value3);
        multiSlice2 = newMultiSlice(tableMetadata, 1, Operator.LTE, value4, value5);
        restrictions = restrictions(tableMetadata, multiSlice2, singleEq, multiSlice);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2, value3);
        assertEndBound(slice, true, value1, value4, value5);
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
        Restriction singleSlice = newSingleRestriction(tableMetadata, 2, Operator.GT, value3);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, multiEq, singleSlice);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());

        Slice slice = slices.get(0);
        assertStartBound(slice, false, value1, value2, value3);
        assertEndBound(slice, true, value1, value2);
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
        Restriction multiSlice = newMultiSlice(tableMetadata, 2, Operator.GT, value3, value4);
        ClusteringColumnRestrictions restrictions = restrictions(tableMetadata, multiEq, multiSlice);

        Slices slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());

        Slice slice = slices.get(0);
        assertEndBound(slice, true, value1, value2);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) IN ((3, 4), (4, 5))
        multiEq = newMultiEq(tableMetadata, 0, value1, value2);
        Restriction multiIN = newMultiIN(tableMetadata, 2, asList(value3, value4), asList(value4, value5));
        restrictions = restrictions(tableMetadata, multiEq, multiIN);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(2, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2, value3, value4);
        assertEndBound(slice, true, value1, value2, value3, value4);

        slice = slices.get(1);
        assertStartBound(slice, true, value1, value2, value4, value5);
        assertEndBound(slice, true, value1, value2, value4, value5);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) = (3, 4)
        multiEq = newMultiEq(tableMetadata, 0, value1, value2);
        Restriction multiEq2 = newMultiEq(tableMetadata, 2, value3, value4);
        restrictions = restrictions(tableMetadata, multiEq, multiEq2);

        slices = restrictions.slices(QueryOptions.DEFAULT);
        assertEquals(1, slices.size());

        slice = slices.get(0);
        assertStartBound(slice, true, value1, value2, value3, value4);
        assertEndBound(slice, true, value1, value2, value3, value4);
    }

    /**
     * Asserts that the start bound of the specified slice is empty.
     *
     * @param slice the slice to check
     */
    private static void assertEmptyStart(Slice slice)
    {
        assertTrue(slice.start().isBottom());
    }

    /**
     * Asserts that the end bound of the specified slice is empty.
     *
     * @param slice the slice to check
     */
    private static void assertEmptyEnd(Slice slice)
    {
        assertTrue(slice.end().isTop());
    }

    /**
     * Asserts that the start bound of the specified slice has the specified value and type (inclusive or not).
     *
     * @param slice the slice to check
     * @param isInclusive if the bound is expected to be inclusive
     * @param elements the expected elements of the clustering
     */
    private static void assertStartBound(Slice slice, boolean isInclusive, ByteBuffer... elements)
    {
        assertBound(slice.start(), true, isInclusive, elements);
    }

    /**
     * Asserts that the end bound of the specified slice has the specified value and type (inclusive or not).
     *
     * @param slice the slice to check
     * @param isInclusive if the bound is expected to be inclusive
     * @param elements the expected elements of the clustering
     */
    private static void assertEndBound(Slice slice, boolean isInclusive, ByteBuffer... elements)
    {
        assertBound(slice.end(), false, isInclusive, elements);
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
     * Creates a new <code>SingleColumnRestriction</code> instance for the specified clustering column.
     *
     * @param tableMetadata the column family meta data
     * @param index the clustering column index
     * @param operator the restriction operator
     * @param values the values
     * @return a new <code>SingleColumnRestriction</code> instance for the specified clustering column
     */
    private static Restriction newSingleRestriction(TableMetadata tableMetadata, int index, Operator operator, ByteBuffer... values)
    {
        ColumnMetadata column = getClusteringColumnDefinition(tableMetadata, index);
        return new SimpleRestriction(ColumnsExpression.singleColumn(column), operator, toTerms(values));
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
        List<ColumnMetadata> columns = new ArrayList<>();
        List<AbstractType<?>> types = new ArrayList<>();
        for (int i = 0; i < values.length; i++)
        {
            ColumnMetadata column = getClusteringColumnDefinition(tableMetadata, firstIndex + i);
            columns.add(column);
            types.add(column.type);
        }
        TupleType tupleType = new TupleType(types);
        return new SimpleRestriction(ColumnsExpression.multiColumns(columns),
                                     Operator.EQ,
                                     Terms.of(new MultiElements.Value(tupleType, asList(values))));
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
        List<ColumnMetadata> columns = new ArrayList<>();
        List<AbstractType<?>> types = new ArrayList<>();

        for (int i = 0; i < values[0].size(); i++)
        {
            ColumnMetadata column = getClusteringColumnDefinition(tableMetadata, firstIndex + i);
            columns.add(column);
            types.add(column.type);
        }

        TupleType tupleType = new TupleType(types);

        List<Term> terms = new ArrayList<>(values.length);
        for (int i = 0; i < values.length; i++)
        {
            terms.add(new MultiElements.Value(tupleType, values[i]));
        }
        return new SimpleRestriction(ColumnsExpression.multiColumns(columns), Operator.IN, Terms.of(terms));
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
     * @param firstIndex the clustering column index
     * @param operator the slice operator
     * @param values the bound value
     * @return a new <code>SingleColumnRestriction.Slice</code> instance for the specified clustering column
     */
    private static Restriction newMultiSlice(TableMetadata tableMetadata, int firstIndex, Operator operator, ByteBuffer... values)
    {
        List<ColumnMetadata> columns = new ArrayList<>(values.length);
        List<AbstractType<?>> types = new ArrayList<>(values.length);
        for (int i = 0; i < values.length; i++)
        {
            ColumnMetadata column = getClusteringColumnDefinition(tableMetadata, i + firstIndex);
            columns.add(column);
            types.add(column.type);
        }
        TupleType type = new TupleType(types);
        return new SimpleRestriction(ColumnsExpression.multiColumns(columns),
                                     operator,
                                     Terms.of(new MultiElements.Value(type, asList(values))));
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
    private static Terms toTerms(ByteBuffer... values)
    {
        List<Term> terms = new ArrayList<>();
        for (ByteBuffer value : values)
            terms.add(toTerm(value));
        return Terms.of(terms);
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
