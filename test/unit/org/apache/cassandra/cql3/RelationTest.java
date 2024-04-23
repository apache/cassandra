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

package org.apache.cassandra.cql3;

import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.functions.FunctionCall;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.terms.Constants;
import org.apache.cassandra.cql3.terms.InMarker;
import org.apache.cassandra.cql3.terms.Marker;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.cql3.terms.Terms;
import org.apache.cassandra.cql3.terms.Tuples;

import static java.util.Arrays.asList;
import static org.apache.cassandra.cql3.Relation.mapElement;
import static org.apache.cassandra.cql3.Relation.multiColumns;
import static org.apache.cassandra.cql3.Relation.singleColumn;
import static org.apache.cassandra.cql3.Relation.token;
import static org.junit.Assert.assertEquals;

public class RelationTest
{
    @Test
    public void toCQLStringTest()
    {
        ColumnIdentifier col = new ColumnIdentifier("col", false);
        ColumnIdentifier col2 = new ColumnIdentifier("col2", false);
        Marker.Raw marker = new Marker.Raw(0);
        InMarker.Raw inMarker = new InMarker.Raw(0);
        Term.Raw one = Constants.Literal.integer("1");
        Term.Raw two = Constants.Literal.integer("2");
        Term.Raw three = Constants.Literal.integer("3");
        Terms.Raw oneTwo = Terms.Raw.of(asList(one, two));
        Term.Raw text = Constants.Literal.string("text");

        assertEquals("col = ?", singleColumn(col, Operator.EQ, marker).toCQLString());
        assertEquals("col = 2", singleColumn(col, Operator.EQ, two).toCQLString());
        assertEquals("col = 'text'", singleColumn(col, Operator.EQ, text).toCQLString());
        assertEquals("col >= ?", singleColumn(col, Operator.GTE, marker).toCQLString());
        assertEquals("col IN ?", singleColumn(col, Operator.IN, inMarker).toCQLString());
        assertEquals("col IN (1, 2)", singleColumn(col, Operator.IN, oneTwo).toCQLString());
        assertEquals("col IN (1)", singleColumn(col, Operator.IN, Terms.Raw.of(List.of(one))).toCQLString());

        Term.Raw tuple1 = new Tuples.Literal(asList(one, two));
        Term.Raw tuple2 = new Tuples.Literal(asList(two, three));
        Terms.Raw tuples = Terms.Raw.of(asList(tuple1, tuple2));

        assertEquals("(col, col2) = ?", multiColumns(asList(col, col2), Operator.EQ, marker).toCQLString());
        assertEquals("(col, col2) = (1, 2)", multiColumns(asList(col, col2), Operator.EQ, tuple1).toCQLString());
        assertEquals("(col, col2) IN ((1, 2), (2, 3))", multiColumns(asList(col, col2), Operator.IN, tuples).toCQLString());
        assertEquals("(col, col2) IN ?", multiColumns(asList(col, col2), Operator.IN, inMarker).toCQLString());

        Term.Raw tokenCall = new FunctionCall.Raw(FunctionName.nativeFunction("token"), asList(one, two));

        assertEquals("token(col, col2) = ?", token(asList(col, col2), Operator.EQ, marker).toCQLString());
        assertEquals("token(col, col2) = token(1, 2)", token(asList(col, col2), Operator.EQ, tokenCall).toCQLString());

        assertEquals("col['text'] = ?", mapElement(col, text, Operator.EQ, marker).toCQLString());
        assertEquals("col[?] = ?", mapElement(col, marker, Operator.EQ, marker).toCQLString());
    }
}
