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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.Term.MultiItemTerminal;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.Composite.EOC;
import org.apache.cassandra.db.composites.Composites;
import org.apache.cassandra.db.composites.CompoundSparseCellNameType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class SelectStatementTest
{
    @Test
    public void testBuildBoundWithNoRestrictions() throws InvalidRequestException
    {
        Restriction[] restrictions = new Restriction[2];

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));
    }

    /**
     * Test 'clustering_0 = 1' with only one clustering column
     */
    @Test
    public void testBuildBoundWithOneEqRestrictionsAndOneClusteringColumn() throws InvalidRequestException
    {
        ByteBuffer clustering_0 = ByteBufferUtil.bytes(1);
        SingleColumnRestriction.EQ eq = new SingleColumnRestriction.EQ(toTerm(clustering_0), false);
        Restriction[] restrictions = new Restriction[] { eq };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), clustering_0, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), clustering_0, EOC.END);
    }

    /**
     * Test 'clustering_1 = 1' with 2 clustering columns
     */
    @Test
    public void testBuildBoundWithOneEqRestrictionsAndTwoClusteringColumns() throws InvalidRequestException
    {
        ByteBuffer clustering_2 = ByteBufferUtil.bytes(1);
        SingleColumnRestriction.EQ eq = new SingleColumnRestriction.EQ(toTerm(clustering_2), false);
        Restriction[] restrictions = new Restriction[] { eq, null };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), clustering_2, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), clustering_2, EOC.END);
    }

    /**
     * Test 'clustering_0 IN (1, 2, 3)' with only one clustering column
     */
    @Test
    public void testBuildBoundWithOneInRestrictionsAndOneClusteringColumn() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        SingleColumnRestriction.IN in = new SingleColumnRestriction.InWithValues(toTerms(value1, value2, value3));
        Restriction[] restrictions = new Restriction[] { in };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(3, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.START);
        assertComposite(bounds.get(1), value2, EOC.START);
        assertComposite(bounds.get(2), value3, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(3, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.END);
        assertComposite(bounds.get(1), value2, EOC.END);
        assertComposite(bounds.get(2), value3, EOC.END);
    }

    /**
     * Test slice restriction (e.g 'clustering_0 > 1') with only one clustering column
     */
    @Test
    public void testBuildBoundWithSliceRestrictionsAndOneClusteringColumn() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        SingleColumnRestriction.Slice slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Operator.GT, toTerm(value1));
        Restriction[] restrictions = new Restriction[] { slice };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.END);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Operator.GTE, toTerm(value1));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.NONE);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Operator.LTE, toTerm(value1));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.END);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Operator.LT, toTerm(value1));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.START);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Operator.GT, toTerm(value1));
        slice.setBound(Operator.LT, toTerm(value2));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.END);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value2, EOC.START);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Operator.GTE, toTerm(value1));
        slice.setBound(Operator.LTE, toTerm(value1));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.NONE);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.END);
    }

    /**
     * Test 'clustering_0 = 1 AND clustering_1 IN (1, 2, 3)' with two clustering columns
     */
    @Test
    public void testBuildBoundWithEqAndInRestrictions() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        SingleColumnRestriction.EQ eq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        SingleColumnRestriction.IN in = new SingleColumnRestriction.InWithValues(toTerms(value1, value2, value3));
        Restriction[] restrictions = new Restriction[] { eq, in };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(3, bounds.size());
        assertComposite(bounds.get(0), value1, value1, EOC.START);
        assertComposite(bounds.get(1), value1, value2, EOC.START);
        assertComposite(bounds.get(2), value1, value3, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(3, bounds.size());
        assertComposite(bounds.get(0), value1, value1, EOC.END);
        assertComposite(bounds.get(1), value1, value2, EOC.END);
        assertComposite(bounds.get(2), value1, value3, EOC.END);
    }

    /**
     * Test slice restriction (e.g 'clustering_0 > 1') with only one clustering column
     */
    @Test
    public void testBuildBoundWithEqAndSliceRestrictions() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);

        SingleColumnRestriction.EQ eq = new SingleColumnRestriction.EQ(toTerm(value3), false);

        SingleColumnRestriction.Slice slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Operator.GT, toTerm(value1));
        Restriction[] restrictions = new Restriction[] { eq, slice };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value3, value1, EOC.END);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value3, EOC.END);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Operator.GTE, toTerm(value1));
        restrictions = new Restriction[] { eq, slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value3, value1, EOC.NONE);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value3, EOC.END);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Operator.LTE, toTerm(value1));
        restrictions = new Restriction[] { eq, slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value3, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value3, value1, EOC.END);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Operator.LT, toTerm(value1));
        restrictions = new Restriction[] { eq, slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value3, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value3, value1, EOC.START);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Operator.GT, toTerm(value1));
        slice.setBound(Operator.LT, toTerm(value2));
        restrictions = new Restriction[] { eq, slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value3, value1, EOC.END);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value3, value2, EOC.START);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Operator.GTE, toTerm(value1));
        slice.setBound(Operator.LTE, toTerm(value1));
        restrictions = new Restriction[] { eq, slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value3, value1, EOC.NONE);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value3, value1, EOC.END);
    }

    /**
     * Test '(clustering_0, clustering_1) = (1, 2)' with two clustering column
     */
    @Test
    public void testBuildBoundWithMultiEqRestrictions() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        MultiColumnRestriction.EQ eq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value1, value2), false);
        Restriction[] restrictions = new Restriction[] { eq, eq };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, EOC.END);
    }

    /**
     * Test '(clustering_0, clustering_1) IN ((1, 2), (2, 3))' with two clustering column
     */
    @Test
    public void testBuildBoundWithMultiInRestrictions() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        List<MultiItemTerminal> terms = asList(toMultiItemTerminal(value1, value2), toMultiItemTerminal(value2, value3));
        MultiColumnRestriction.IN in = new MultiColumnRestriction.InWithValues(terms);
        Restriction[] restrictions = new Restriction[] { in, in };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(2, bounds.size());
        assertComposite(bounds.get(0), value1, value2, EOC.START);
        assertComposite(bounds.get(1), value2, value3, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(2, bounds.size());
        assertComposite(bounds.get(0), value1, value2, EOC.END);
        assertComposite(bounds.get(1), value2, value3, EOC.END);
    }

    /**
     * Test multi-column slice restrictions (e.g '(clustering_0) > (1)') with only one clustering column
     */
    @Test
    public void testBuildBoundWithMultiSliceRestrictionsWithOneClusteringColumn() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        MultiColumnRestriction.Slice slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Operator.GT, toMultiItemTerminal(value1));
        Restriction[] restrictions = new Restriction[] { slice };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.END);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));

        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Operator.GTE, toMultiItemTerminal(value1));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.NONE);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));

        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Operator.LTE, toMultiItemTerminal(value1));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.END);

        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Operator.LT, toMultiItemTerminal(value1));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.START);

        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Operator.GT, toMultiItemTerminal(value1));
        slice.setBound(Operator.LT, toMultiItemTerminal(value2));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.END);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value2, EOC.START);

        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Operator.GTE, toMultiItemTerminal(value1));
        slice.setBound(Operator.LTE, toMultiItemTerminal(value2));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.NONE);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value2, EOC.END);
    }

    /**
     * Test multi-column slice restrictions (e.g '(clustering_0, clustering_1) > (1, 2)') with only one clustering
     * column
     */
    @Test
    public void testBuildBoundWithMultiSliceRestrictionsWithTwoClusteringColumn() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        // (clustering_0, clustering1) > (1, 2)
        MultiColumnRestriction.Slice slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Operator.GT, toMultiItemTerminal(value1, value2));
        Restriction[] restrictions = new Restriction[] { slice, slice };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, EOC.END);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));

        // (clustering_0, clustering1) >= (1, 2)
        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Operator.GTE, toMultiItemTerminal(value1, value2));
        restrictions = new Restriction[] { slice, slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, EOC.NONE);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));

        // (clustering_0, clustering1) <= (1, 2)
        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Operator.LTE, toMultiItemTerminal(value1, value2));
        restrictions = new Restriction[] { slice, slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, EOC.END);

        // (clustering_0, clustering1) < (1, 2)
        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Operator.LT, toMultiItemTerminal(value1, value2));
        restrictions = new Restriction[] { slice, slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertEmptyComposite(bounds.get(0));

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, EOC.START);

        // (clustering_0, clustering1) > (1, 2) AND (clustering_0) < (2)
        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Operator.GT, toMultiItemTerminal(value1, value2));
        slice.setBound(Operator.LT, toMultiItemTerminal(value2));
        restrictions = new Restriction[] { slice, slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, EOC.END);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value2, EOC.START);

        // (clustering_0, clustering1) >= (1, 2) AND (clustering_0, clustering1) <= (2, 1)
        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Operator.GTE, toMultiItemTerminal(value1, value2));
        slice.setBound(Operator.LTE, toMultiItemTerminal(value2, value1));
        restrictions = new Restriction[] { slice, slice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, EOC.NONE);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value2, value1, EOC.END);
    }

    /**
     * Test mixing single and multi equals restrictions (e.g. clustering_0 = 1 AND (clustering_1, clustering_2) = (2, 3))
     */
    @Test
    public void testBuildBoundWithSingleEqAndMultiEqRestrictions() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);

        // clustering_0 = 1 AND (clustering_1, clustering_2) = (2, 3)
        SingleColumnRestriction.EQ singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        MultiColumnRestriction.EQ multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value2, value3), false);
        Restriction[] restrictions = new Restriction[] { singleEq, multiEq, multiEq };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, EOC.END);

        // clustering_0 = 1 AND clustering_1 = 2 AND (clustering_2, clustering_3) = (3, 4)
        singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        SingleColumnRestriction.EQ singleEq2 = new SingleColumnRestriction.EQ(toTerm(value2), false);
        multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value3, value4), false);
        restrictions = new Restriction[] { singleEq, singleEq2, multiEq, multiEq };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, value4, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, value4, EOC.END);

        // (clustering_0, clustering_1) = (1, 2) AND clustering_2 = 3
        singleEq = new SingleColumnRestriction.EQ(toTerm(value3), false);
        multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value1, value2), false);
        restrictions = new Restriction[] { multiEq, multiEq, singleEq };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, EOC.END);

        // clustering_0 = 1 AND (clustering_1, clustering_2) = (2, 3) AND clustering_3 = 4
        singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        singleEq2 = new SingleColumnRestriction.EQ(toTerm(value4), false);
        multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value2, value3), false);
        restrictions = new Restriction[] { singleEq, multiEq, multiEq, singleEq2 };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, value4, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, value4, EOC.END);
    }

    /**
     * Test clustering_0 = 1 AND (clustering_1, clustering_2) IN ((2, 3), (4, 5))
     */
    @Test
    public void testBuildBoundWithSingleEqAndMultiINRestrictions() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);
        ByteBuffer value5 = ByteBufferUtil.bytes(5);

        // clustering_0 = 1 AND (clustering_1, clustering_2) IN ((2, 3), (4, 5))
        SingleColumnRestriction.EQ singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        MultiColumnRestriction.IN multiIn =
                new MultiColumnRestriction.InWithValues(asList(toMultiItemTerminal(value2, value3),
                                                               toMultiItemTerminal(value4, value5)));

        Restriction[] restrictions = new Restriction[] { singleEq, multiIn, multiIn };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(2, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, EOC.START);
        assertComposite(bounds.get(1), value1, value4, value5, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(2, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, EOC.END);
        assertComposite(bounds.get(1), value1, value4, value5, EOC.END);

        // clustering_0 = 1 AND (clustering_1, clustering_2) IN ((2, 3))
        singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        multiIn = new MultiColumnRestriction.InWithValues(asList(toMultiItemTerminal(value2, value3),
                                                                 toMultiItemTerminal(value4, value5)));

        restrictions = new Restriction[] { singleEq, multiIn, multiIn };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(2, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, EOC.START);
        assertComposite(bounds.get(1), value1, value4, value5, EOC.START);

        // clustering_0 = 1 AND clustering_1 = 5 AND (clustering_2, clustering_3) IN ((2, 3), (4, 5))
        singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        SingleColumnRestriction.EQ singleEq2 = new SingleColumnRestriction.EQ(toTerm(value5), false);
        multiIn = new MultiColumnRestriction.InWithValues(asList(toMultiItemTerminal(value2, value3),
                                                                 toMultiItemTerminal(value4, value5)));

        restrictions = new Restriction[] { singleEq, singleEq2, multiIn, multiIn };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(2, bounds.size());
        assertComposite(bounds.get(0), value1, value5, value2, value3, EOC.START);
        assertComposite(bounds.get(1), value1, value5, value4, value5, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(2, bounds.size());
        assertComposite(bounds.get(0), value1, value5, value2, value3, EOC.END);
        assertComposite(bounds.get(1), value1, value5, value4, value5, EOC.END);
    }

    /**
     * Test mixing single equal restrictions with multi-column slice restrictions
     * (e.g. clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3))
     */
    @Test
    public void testBuildBoundWithSingleEqAndSliceRestrictions() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);
        ByteBuffer value5 = ByteBufferUtil.bytes(5);

        // clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3)
        SingleColumnRestriction.EQ singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        MultiColumnRestriction.Slice multiSlice = new MultiColumnRestriction.Slice(false);
        multiSlice.setBound(Operator.GT, toMultiItemTerminal(value2, value3));

        Restriction[] restrictions = new Restriction[] { singleEq, multiSlice, multiSlice };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, EOC.END);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, EOC.END);

        // clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3) AND (clustering_1) < (4)
        singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        multiSlice = new MultiColumnRestriction.Slice(false);
        multiSlice.setBound(Operator.GT, toMultiItemTerminal(value2, value3));
        multiSlice.setBound(Operator.LT, toMultiItemTerminal(value4));

        restrictions = new Restriction[] { singleEq, multiSlice, multiSlice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, EOC.END);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value4, EOC.START);

        // clustering_0 = 1 AND (clustering_1, clustering_2) => (2, 3) AND (clustering_1, clustering_2) <= (4, 5)
        singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        multiSlice = new MultiColumnRestriction.Slice(false);
        multiSlice.setBound(Operator.GTE, toMultiItemTerminal(value2, value3));
        multiSlice.setBound(Operator.LTE, toMultiItemTerminal(value4, value5));

        restrictions = new Restriction[] { singleEq, multiSlice, multiSlice };

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, EOC.NONE);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value4, value5, EOC.END);
    }

    /**
     * Test mixing multi equal restrictions with single-column slice restrictions
     * (e.g. clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3))
     */
    @Test
    public void testBuildBoundWithMultiEqAndSingleSliceRestrictions() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);

        // (clustering_0, clustering_1) = (1, 2) AND clustering_2 > 3
        MultiColumnRestriction.EQ multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value1, value2), false);
        SingleColumnRestriction.Slice singleSlice = new SingleColumnRestriction.Slice(false);
        singleSlice.setBound(Operator.GT, toTerm(value3));

        Restriction[] restrictions = new Restriction[] { multiEq, multiEq, singleSlice };

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, EOC.END);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0),  value1, value2, EOC.END);
    }

    @Test
    public void testBuildBoundWithSeveralMultiColumnRestrictions() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);
        ByteBuffer value5 = ByteBufferUtil.bytes(5);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) > (3, 4)
        MultiColumnRestriction.EQ multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value1, value2), false);
        MultiColumnRestriction.Slice multiSlice = new MultiColumnRestriction.Slice(false);
        multiSlice.setBound(Operator.GT, toMultiItemTerminal(value3, value4));

        Restriction[] restrictions = new Restriction[] { multiEq, multiEq, multiSlice, multiSlice};

        List<Composite> bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, value4, EOC.END);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0),  value1, value2, EOC.END);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) IN ((3, 4), (4, 5))
        multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value1, value2), false);
        MultiColumnRestriction.IN multiIn =
                new MultiColumnRestriction.InWithValues(asList(toMultiItemTerminal(value3, value4),
                                                               toMultiItemTerminal(value4, value5)));

        restrictions = new Restriction[] { multiEq, multiEq, multiIn, multiIn};

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(2, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, value4, EOC.START);
        assertComposite(bounds.get(1), value1, value2, value4, value5, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(2, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, value4, EOC.END);
        assertComposite(bounds.get(1), value1, value2, value4, value5, EOC.END);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) IN ((3, 4), (4, 5))
        multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value1, value2), false);
        MultiColumnRestriction.EQ multiEq2 = new MultiColumnRestriction.EQ(toMultiItemTerminal(value3, value4), false);

        restrictions = new Restriction[] { multiEq, multiEq, multiEq2, multiEq2};

        bounds = executeBuildBound(restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, value4, EOC.START);

        bounds = executeBuildBound(restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(bounds.get(0), value1, value2, value3, value4, EOC.END);
    }

    /**
     * Asserts that the specified <code>Composite</code> is an empty one.
     *
     * @param composite the composite to check
     */
    private static void assertEmptyComposite(Composite composite)
    {
        assertEquals(Composites.EMPTY, composite);
    }

    /**
     * Asserts that the specified <code>Composite</code> contains the specified element and the specified EOC.
     *
     * @param composite the composite to check
     * @param element the expected element of the composite
     * @param eoc the expected EOC of the composite
     */
    private static void assertComposite(Composite composite, ByteBuffer element, EOC eoc)
    {
        assertComposite(composite, eoc, element);
    }

    /**
     * Asserts that the specified <code>Composite</code> contains the 2 specified element and the specified EOC.
     *
     * @param composite the composite to check
     * @param eoc the expected EOC of the composite
     * @param elements the expected element of the composite
     */
    private static void assertComposite(Composite composite, ByteBuffer firstElement, ByteBuffer secondElement, EOC eoc)
    {
        assertComposite(composite, eoc, firstElement, secondElement);
    }

    /**
     * Asserts that the specified <code>Composite</code> contains the 3 specified element and the specified EOC.
     *
     * @param composite the composite to check
     * @param firstElement the first expected element of the composite
     * @param secondElement the second expected element of the composite
     * @param thirdElement the third expected element of the composite
     * @param eoc the expected EOC of the composite
     * @param elements the expected element of the composite
     */
    private static void assertComposite(Composite composite,
                                        ByteBuffer firstElement,
                                        ByteBuffer secondElement,
                                        ByteBuffer thirdElement,
                                        EOC eoc)
    {
        assertComposite(composite, eoc, firstElement, secondElement, thirdElement);
    }

    /**
     * Asserts that the specified <code>Composite</code> contains the 4 specified element and the specified EOC.
     *
     * @param composite the composite to check
     * @param firstElement the first expected element of the composite
     * @param secondElement the second expected element of the composite
     * @param thirdElement the third expected element of the composite
     * @param fourthElement the fourth expected element of the composite
     * @param eoc the expected EOC of the composite
     * @param elements the expected element of the composite
     */
    private static void assertComposite(Composite composite,
                                        ByteBuffer firstElement,
                                        ByteBuffer secondElement,
                                        ByteBuffer thirdElement,
                                        ByteBuffer fourthElement,
                                        EOC eoc)
    {
        assertComposite(composite, eoc, firstElement, secondElement, thirdElement, fourthElement);
    }

    /**
     * Asserts that the specified <code>Composite</code> contains the specified elements and EOC.
     *
     * @param composite the composite to check
     * @param eoc the expected EOC of the composite
     * @param elements the expected elements of the composite
     */
    private static void assertComposite(Composite composite, EOC eoc, ByteBuffer... elements)
    {
        assertEquals("the composite size is not the expected one:", elements.length, composite.size());
        for (int i = 0, m = elements.length; i < m; i++)
        {
            ByteBuffer element = elements[i];
            assertEquals("the element " + i + " of the composite is not the expected one:", element, composite.get(i));
        }
        assertEquals("the EOC of the composite is not the expected one:", eoc, composite.eoc());
    }

    /**
     * Calls the <code>SelectStatement.buildBound</code> with the specified restrictions.
     *
     * @param restrictions the restrictions
     * @return the result from the method call to <code>SelectStatement.buildBound</code>
     * @throws InvalidRequestException if the method call throw an exception
     */
    private static List<Composite> executeBuildBound(Restriction[] restrictions,
                                                     Bound bound) throws InvalidRequestException
    {
        List<AbstractType<?>> types = new ArrayList<>();

        for (int i = 0, m = restrictions.length; i < m; i++)
            types.add(Int32Type.instance);

        CompoundSparseCellNameType cType = new CompoundSparseCellNameType(types);
        CFMetaData cfMetaData = new CFMetaData("keyspace", "test", ColumnFamilyType.Standard, cType);

        List<ColumnDefinition> columnDefs = new ArrayList<>();
        for (int i = 0, m = restrictions.length; i < m; i++)
        {
            ByteBuffer name = ByteBufferUtil.bytes("clustering_" + i);
            columnDefs.add(ColumnDefinition.clusteringKeyDef(cfMetaData, name, types.get(i), i));
        }

        return SelectStatement.buildBound(bound, columnDefs, restrictions, false, cType, QueryOptions.DEFAULT);
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
}
