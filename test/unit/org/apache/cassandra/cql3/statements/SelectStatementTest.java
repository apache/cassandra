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
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.CFDefinition.Name;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertArrayEquals;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class SelectStatementTest
{
    @Test
    public void testBuildBoundWithNoRestrictions() throws Exception
    {
        Restriction[] restrictions = new Restriction[2];
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));
    }

    /**
     * Test 'clustering_0 = 1' with only one clustering column
     */
    @Test
    public void testBuildBoundWithOneEqRestrictionsAndOneClusteringColumn() throws Exception
    {
        ByteBuffer clustering_0 = ByteBufferUtil.bytes(1);
        SingleColumnRestriction.EQ eq = new SingleColumnRestriction.EQ(toTerm(clustering_0), false);
        Restriction[] restrictions = new Restriction[] { eq };
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), clustering_0);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), clustering_0);
    }

    /**
     * Test 'clustering_1 = 1' with 2 clustering columns
     */
    @Test
    public void testBuildBoundWithOneEqRestrictionsAndTwoClusteringColumns() throws Exception
    {
        ByteBuffer clustering_0 = ByteBufferUtil.bytes(1);
        SingleColumnRestriction.EQ eq = new SingleColumnRestriction.EQ(toTerm(clustering_0), false);
        Restriction[] restrictions = new Restriction[] { eq, null };
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), clustering_0);

        bounds = executeBuildBound(cfDef,restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertEndOfRangeComposite(cfDef, bounds.get(0), clustering_0);
    }

    /**
     * Test 'clustering_0 IN (1, 2, 3)' with only one clustering column
     */
    @Test
    public void testBuildBoundWithOneInRestrictionsAndOneClusteringColumn() throws Exception
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        SingleColumnRestriction.IN in = new SingleColumnRestriction.InWithValues(toTerms(value1, value2, value3));
        Restriction[] restrictions = new Restriction[] { in };
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef,restrictions, Bound.START);
        assertEquals(3, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1);
        assertComposite(cfDef, bounds.get(1), value2);
        assertComposite(cfDef, bounds.get(2), value3);

        bounds = executeBuildBound(cfDef,restrictions, Bound.END);
        assertEquals(3, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1);
        assertComposite(cfDef, bounds.get(1), value2);
        assertComposite(cfDef, bounds.get(2), value3);
    }

    /**
     * Test slice restriction (e.g 'clustering_0 > 1') with only one clustering column
     */
    @Test
    public void testBuildBoundWithSliceRestrictionsAndOneClusteringColumn() throws Exception
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        SingleColumnRestriction.Slice slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GT, toTerm(value1));
        Restriction[] restrictions = new Restriction[] { slice };
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef,restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GT, value1);

        bounds = executeBuildBound(cfDef,restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GTE, toTerm(value1));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(cfDef,restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GTE, value1);

        bounds = executeBuildBound(cfDef,restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.LTE, toTerm(value1));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(cfDef,restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));

        bounds = executeBuildBound(cfDef,restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LTE, value1);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.LT, toTerm(value1));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(cfDef,restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));

        bounds = executeBuildBound(cfDef,restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LT, value1);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GT, toTerm(value1));
        slice.setBound(Relation.Type.LT, toTerm(value2));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(cfDef,restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GT, value1);

        bounds = executeBuildBound(cfDef,restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LT, value2);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GTE, toTerm(value1));
        slice.setBound(Relation.Type.LTE, toTerm(value2));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(cfDef,restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GTE, value1);

        bounds = executeBuildBound(cfDef,restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LTE, value2);
    }
//
    /**
     * Test 'clustering_0 = 1 AND clustering_1 IN (1, 2, 3)' with two clustering columns
     */
    @Test
    public void testBuildBoundWithEqAndInRestrictions() throws Exception
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        SingleColumnRestriction.EQ eq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        SingleColumnRestriction.IN in = new SingleColumnRestriction.InWithValues(toTerms(value1, value2, value3));
        Restriction[] restrictions = new Restriction[] { eq, in };
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef,restrictions, Bound.START);
        assertEquals(3, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value1);
        assertComposite(cfDef, bounds.get(1), value1, value2);
        assertComposite(cfDef, bounds.get(2), value1, value3);

        bounds = executeBuildBound(cfDef,restrictions, Bound.END);
        assertEquals(3, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value1);
        assertComposite(cfDef, bounds.get(1), value1, value2);
        assertComposite(cfDef, bounds.get(2), value1, value3);
    }

    /**
     * Test slice restriction (e.g 'clustering_0 > 1') with only one clustering column
     */
    @Test
    public void testBuildBoundWithEqAndSliceRestrictions() throws Exception
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);

        SingleColumnRestriction.EQ eq = new SingleColumnRestriction.EQ(toTerm(value3), false);

        SingleColumnRestriction.Slice slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GT, toTerm(value1));
        Restriction[] restrictions = new Restriction[] { eq, slice };
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef,restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GT, value3, value1);

        bounds = executeBuildBound(cfDef,restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertEndOfRangeComposite(cfDef, bounds.get(0), value3);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GTE, toTerm(value1));
        restrictions = new Restriction[] { eq, slice };

        bounds = executeBuildBound(cfDef,restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GTE, value3, value1);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertEndOfRangeComposite(cfDef, bounds.get(0), value3);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.LTE, toTerm(value1));
        restrictions = new Restriction[] { eq, slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value3);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LTE, value3, value1);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.LT, toTerm(value1));
        restrictions = new Restriction[] { eq, slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value3);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LT, value3, value1);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GT, toTerm(value1));
        slice.setBound(Relation.Type.LT, toTerm(value2));
        restrictions = new Restriction[] { eq, slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GT, value3, value1);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LT, value3, value2);

        slice = new SingleColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GTE, toTerm(value1));
        slice.setBound(Relation.Type.LTE, toTerm(value2));
        restrictions = new Restriction[] { eq, slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GTE, value3, value1);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LTE, value3, value2);
    }

    /**
     * Test '(clustering_0, clustering_1) = (1, 2)' with two clustering column
     */
    @Test
    public void testBuildBoundWithMultiEqRestrictions() throws Exception
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        MultiColumnRestriction.EQ eq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value1, value2), false);
        Restriction[] restrictions = new Restriction[] { eq, eq };
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2);
    }

    /**
     * Test '(clustering_0, clustering_1) IN ((1, 2), (2, 3))' with two clustering column
     */
    @Test
    public void testBuildBoundWithMultiInRestrictions() throws Exception
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        List<Term> terms = asList(toMultiItemTerminal(value1, value2), toMultiItemTerminal(value2, value3));
        MultiColumnRestriction.IN in = new MultiColumnRestriction.InWithValues(terms);
        Restriction[] restrictions = new Restriction[] { in, in };
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(2, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2);
        assertComposite(cfDef, bounds.get(1), value2, value3);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(2, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2);
        assertComposite(cfDef, bounds.get(1), value2, value3);
    }

    /**
     * Test multi-column slice restrictions (e.g '(clustering_0) > (1)') with only one clustering column
     */
    @Test
    public void testBuildBoundWithMultiSliceRestrictionsWithOneClusteringColumn() throws Exception
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        MultiColumnRestriction.Slice slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GT, toMultiItemTerminal(value1));
        Restriction[] restrictions = new Restriction[] { slice };
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GT, value1);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));

        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GTE, toMultiItemTerminal(value1));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GTE, value1);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));

        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.LTE, toMultiItemTerminal(value1));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LTE, value1);

        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.LT, toMultiItemTerminal(value1));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LT, value1);

        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GT, toMultiItemTerminal(value1));
        slice.setBound(Relation.Type.LT, toMultiItemTerminal(value2));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GT, value1);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LT, value2);

        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GTE, toMultiItemTerminal(value1));
        slice.setBound(Relation.Type.LTE, toMultiItemTerminal(value2));
        restrictions = new Restriction[] { slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GTE, value1);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LTE, value2);
    }

    /**
     * Test multi-column slice restrictions (e.g '(clustering_0, clustering_1) > (1, 2)') with two clustering
     * columns
     */
    @Test
    public void testBuildBoundWithMultiSliceRestrictionsWithTwoClusteringColumn() throws Exception
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        // (clustering_0, clustering1) > (1, 2)
        MultiColumnRestriction.Slice slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GT, toMultiItemTerminal(value1, value2));
        Restriction[] restrictions = new Restriction[] { slice, slice };
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GT, value1, value2);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));

        // (clustering_0, clustering1) >= (1, 2)
        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GTE, toMultiItemTerminal(value1, value2));
        restrictions = new Restriction[] { slice, slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GTE, value1, value2);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));

        // (clustering_0, clustering1) <= (1, 2)
        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.LTE, toMultiItemTerminal(value1, value2));
        restrictions = new Restriction[] { slice, slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LTE, value1, value2);

        // (clustering_0, clustering1) < (1, 2)
        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.LT, toMultiItemTerminal(value1, value2));
        restrictions = new Restriction[] { slice, slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0));

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LT, value1, value2);

        // (clustering_0, clustering1) > (1, 2) AND (clustering_0) < (2)
        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GT, toMultiItemTerminal(value1, value2));
        slice.setBound(Relation.Type.LT, toMultiItemTerminal(value2));
        restrictions = new Restriction[] { slice, slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GT, value1, value2);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LT, value2);

        // (clustering_0, clustering1) >= (1, 2) AND (clustering_0, clustering1) <= (2, 1)
        slice = new MultiColumnRestriction.Slice(false);
        slice.setBound(Relation.Type.GTE, toMultiItemTerminal(value1, value2));
        slice.setBound(Relation.Type.LTE, toMultiItemTerminal(value2, value1));
        restrictions = new Restriction[] { slice, slice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GTE, value1, value2);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LTE, value2, value1);
    }

    /**
     * Test mixing single and multi equals restrictions (e.g. clustering_0 = 1 AND (clustering_1, clustering_2) = (2, 3))
     */
    @Test
    public void testBuildBoundWithSingleEqAndMultiEqRestrictions() throws Exception
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);

        // clustering_0 = 1 AND (clustering_1, clustering_2) = (2, 3)
        SingleColumnRestriction.EQ singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        MultiColumnRestriction.EQ multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value2, value3), false);
        Restriction[] restrictions = new Restriction[] { singleEq, multiEq, multiEq };
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3);

        // clustering_0 = 1 AND clustering_1 = 2 AND (clustering_2, clustering_3) = (3, 4)
        singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        SingleColumnRestriction.EQ singleEq2 = new SingleColumnRestriction.EQ(toTerm(value2), false);
        multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value3, value4), false);
        restrictions = new Restriction[] { singleEq, singleEq2, multiEq, multiEq };
        cfDef = createCFDefinition(restrictions.length);

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3, value4);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3, value4);

        // (clustering_0, clustering_1) = (1, 2) AND clustering_2 = 3
        singleEq = new SingleColumnRestriction.EQ(toTerm(value3), false);
        multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value1, value2), false);
        restrictions = new Restriction[] { multiEq, multiEq, singleEq };
        cfDef = createCFDefinition(restrictions.length);

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3);

        // clustering_0 = 1 AND (clustering_1, clustering_2) = (2, 3) AND clustering_3 = 4
        singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        singleEq2 = new SingleColumnRestriction.EQ(toTerm(value4), false);
        multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value2, value3), false);
        restrictions = new Restriction[] { singleEq, multiEq, multiEq, singleEq2 };
        cfDef = createCFDefinition(restrictions.length);

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3, value4);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3, value4);
    }

    /**
     * Test clustering_0 = 1 AND (clustering_1, clustering_2) IN ((2, 3), (4, 5))
     */
    @Test
    public void testBuildBoundWithSingleEqAndMultiINRestrictions() throws Exception
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
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(2, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3);
        assertComposite(cfDef, bounds.get(1), value1, value4, value5);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(2, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3);
        assertComposite(cfDef, bounds.get(1), value1, value4, value5);

        // clustering_0 = 1 AND (clustering_1, clustering_2) IN ((2, 3))
        singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        multiIn = new MultiColumnRestriction.InWithValues(asList(toMultiItemTerminal(value2, value3),
                                                                 toMultiItemTerminal(value4, value5)));

        restrictions = new Restriction[] { singleEq, multiIn, multiIn };
        cfDef = createCFDefinition(restrictions.length);

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(2, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3);
        assertComposite(cfDef, bounds.get(1), value1, value4, value5);

        // clustering_0 = 1 AND clustering_1 = 5 AND (clustering_2, clustering_3) IN ((2, 3), (4, 5))
        singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        SingleColumnRestriction.EQ singleEq2 = new SingleColumnRestriction.EQ(toTerm(value5), false);
        multiIn = new MultiColumnRestriction.InWithValues(asList(toMultiItemTerminal(value2, value3),
                                                                 toMultiItemTerminal(value4, value5)));

        restrictions = new Restriction[] { singleEq, singleEq2, multiIn, multiIn };
        cfDef = createCFDefinition(restrictions.length);

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(2, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value5, value2, value3);
        assertComposite(cfDef, bounds.get(1), value1, value5, value4, value5);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(2, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value5, value2, value3);
        assertComposite(cfDef, bounds.get(1), value1, value5, value4, value5);
    }

    /**
     * Test mixing single equal restrictions with multi-column slice restrictions
     * (e.g. clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3))
     */
    @Test
    public void testBuildBoundWithSingleEqAndSliceRestrictions() throws Exception
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);
        ByteBuffer value5 = ByteBufferUtil.bytes(5);

        // clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3)
        SingleColumnRestriction.EQ singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        MultiColumnRestriction.Slice multiSlice = new MultiColumnRestriction.Slice(false);
        multiSlice.setBound(Relation.Type.GT, toMultiItemTerminal(value2, value3));

        Restriction[] restrictions = new Restriction[] { singleEq, multiSlice, multiSlice };
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GT, value1, value2, value3);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertEndOfRangeComposite(cfDef, bounds.get(0), value1);

        // clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3) AND (clustering_1) < (4)
        singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        multiSlice = new MultiColumnRestriction.Slice(false);
        multiSlice.setBound(Relation.Type.GT, toMultiItemTerminal(value2, value3));
        multiSlice.setBound(Relation.Type.LT, toMultiItemTerminal(value4));

        restrictions = new Restriction[] { singleEq, multiSlice, multiSlice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GT, value1, value2, value3);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LT, value1, value4);

        // clustering_0 = 1 AND (clustering_1, clustering_2) => (2, 3) AND (clustering_1, clustering_2) <= (4, 5)
        singleEq = new SingleColumnRestriction.EQ(toTerm(value1), false);
        multiSlice = new MultiColumnRestriction.Slice(false);
        multiSlice.setBound(Relation.Type.GTE, toMultiItemTerminal(value2, value3));
        multiSlice.setBound(Relation.Type.LTE, toMultiItemTerminal(value4, value5));

        restrictions = new Restriction[] { singleEq, multiSlice, multiSlice };

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GTE, value1, value2, value3);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.LTE, value1, value4, value5);
    }

    /**
     * Test mixing multi equal restrictions with single-column slice restrictions
     * (e.g. clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3))
     */
    @Test
    public void testBuildBoundWithMultiEqAndSingleSliceRestrictions() throws Exception
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);

        // (clustering_0, clustering_1) = (1, 2) AND clustering_2 > 3
        MultiColumnRestriction.EQ multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value1, value2), false);
        SingleColumnRestriction.Slice singleSlice = new SingleColumnRestriction.Slice(false);
        singleSlice.setBound(Relation.Type.GT, toTerm(value3));

        Restriction[] restrictions = new Restriction[] { multiEq, multiEq, singleSlice };
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GT, value1, value2, value3);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertEndOfRangeComposite(cfDef, bounds.get(0), value1, value2);
    }

    @Test
    public void testBuildBoundWithSeveralMultiColumnRestrictions() throws Exception
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);
        ByteBuffer value5 = ByteBufferUtil.bytes(5);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) > (3, 4)
        MultiColumnRestriction.EQ multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value1, value2), false);
        MultiColumnRestriction.Slice multiSlice = new MultiColumnRestriction.Slice(false);
        multiSlice.setBound(Relation.Type.GT, toMultiItemTerminal(value3, value4));

        Restriction[] restrictions = new Restriction[] { multiEq, multiEq, multiSlice, multiSlice};
        CFDefinition cfDef = createCFDefinition(restrictions.length);

        List<ByteBuffer> bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertForRelationComposite(cfDef, bounds.get(0), Relation.Type.GT,value1, value2, value3, value4);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertEndOfRangeComposite(cfDef, bounds.get(0),  value1, value2);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) IN ((3, 4), (4, 5))
        multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value1, value2), false);
        MultiColumnRestriction.IN multiIn =
                new MultiColumnRestriction.InWithValues(asList(toMultiItemTerminal(value3, value4),
                                                               toMultiItemTerminal(value4, value5)));

        restrictions = new Restriction[] { multiEq, multiEq, multiIn, multiIn};
        cfDef = createCFDefinition(restrictions.length);

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(2, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3, value4);
        assertComposite(cfDef, bounds.get(1), value1, value2, value4, value5);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(2, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3, value4);
        assertComposite(cfDef, bounds.get(1), value1, value2, value4, value5);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) = ((3, 4), (4, 5))
        multiEq = new MultiColumnRestriction.EQ(toMultiItemTerminal(value1, value2), false);
        MultiColumnRestriction.EQ multiEq2 = new MultiColumnRestriction.EQ(toMultiItemTerminal(value3, value4), false);

        restrictions = new Restriction[] { multiEq, multiEq, multiEq2, multiEq2};
        cfDef = createCFDefinition(restrictions.length);

        bounds = executeBuildBound(cfDef, restrictions, Bound.START);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3, value4);

        bounds = executeBuildBound(cfDef, restrictions, Bound.END);
        assertEquals(1, bounds.size());
        assertComposite(cfDef, bounds.get(0), value1, value2, value3, value4);
    }

    /**
     * Asserts that the specified composite contains the specified elements.
     *
     * @param cfDef the Column Family Definition
     * @param actual the buffer to test
     * @param elements the expected elements of the composite
     */
    private static void assertComposite(CFDefinition cfDef, ByteBuffer actual, ByteBuffer... elements)
    {
        ColumnNameBuilder builder = addElements(cfDef.getColumnNameBuilder(), elements);
        assertArrayEquals("the composite is not the expected one:", actual.array(), builder.build().array());
    }

    /**
     * Asserts that the specified composite is an end of range composite that contains the specified elements.
     *
     * @param cfDef the Column Family Definition
     * @param actual the buffer to test
     * @param elements the expected elements of the composite
     */
    private static void assertEndOfRangeComposite(CFDefinition cfDef, ByteBuffer actual, ByteBuffer... elements)
    {
        ColumnNameBuilder builder = addElements(cfDef.getColumnNameBuilder(), elements);
        assertArrayEquals("the composite is not the expected one:", actual.array(), builder.buildAsEndOfRange().array());
    }

    /**
     * Asserts that the specified composite is an end of range composite that contains the specified elements.
     *
     * @param cfDef the Column Family Definition
     * @param actual the buffer to test
     * @param elements the expected elements of the composite
     */
    private static void assertForRelationComposite(CFDefinition cfDef,
                                                   ByteBuffer actual,
                                                   Relation.Type relType,
                                                   ByteBuffer... elements)
    {
        ColumnNameBuilder builder = addElements(cfDef.getColumnNameBuilder(), elements);
        assertArrayEquals("the composite is not the expected one:", actual.array(), builder.buildForRelation(relType).array());
    }

    /**
     * Adds all the specified elements to the specified builder.
     *
     * @param builder the builder to add to
     * @param elements the elements to add
     * @return the builder
     */
    private static ColumnNameBuilder addElements(ColumnNameBuilder builder, ByteBuffer... elements)
    {
        for (int i = 0, m = elements.length; i < m; i++)
            builder.add(elements[i]);
        return builder;
    }

    /**
     * Calls the <code>SelectStatement.buildBound</code> with the specified restrictions.
     *
     * @param cfDef the Column Family Definition
     * @param restrictions the restrictions
     * @return the result from the method call to <code>SelectStatement.buildBound</code>
     * @throws InvalidRequestException if buildBound throw an <code>Exception</code>
     */
    private static List<ByteBuffer> executeBuildBound(CFDefinition cfDef,
                                                      Restriction[] restrictions,
                                                      Bound bound) throws InvalidRequestException
    {
        return SelectStatement.buildBound(bound,
                                          new ArrayList<Name>(cfDef.clusteringColumns()),
                                          restrictions,
                                          false,
                                          cfDef,
                                          cfDef.getColumnNameBuilder(),
                                          Collections.<ByteBuffer>emptyList());
    }

    /**
     * Creates a <code>CFDefinition</code> to be used in the tests.
     *
     * @param numberOfClusteringColumns the number of clustering columns
     * @return a new a <code>CFDefinition</code> instance
     * @throws ConfigurationException if the CFDefinition cannot be created
     */
    private static CFDefinition createCFDefinition(int numberOfClusteringColumns) throws ConfigurationException
    {
        List<AbstractType<?>> types = new ArrayList<>();
        for (int i = 0, m = numberOfClusteringColumns; i < m; i++)
            types.add(Int32Type.instance);

        CompositeType cType = CompositeType.getInstance(types);
        CFMetaData cfMetaData = new CFMetaData("keyspace", "test", ColumnFamilyType.Standard, cType);
        ByteBuffer partitionKey = ByteBufferUtil.bytes("partitionKey");
        cfMetaData.addColumnDefinition(ColumnDefinition.partitionKeyDef(partitionKey, Int32Type.instance, 0));

        for (int i = 0, m = numberOfClusteringColumns; i < m; i++)
        {
            ByteBuffer name = ByteBufferUtil.bytes("clustering_" + i);
            cfMetaData.addColumnDefinition(ColumnDefinition.clusteringKeyDef(name, types.get(i), i));
        }
        cfMetaData.rebuild();
        return new CFDefinition(cfMetaData);
    }

    /**
     * Converts the specified values into a <code>MultiItemTerminal</code>.
     *
     * @param values the values to convert.
     * @return the term corresponding to the specified values.
     */
    private static Term toMultiItemTerminal(ByteBuffer... values)
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
