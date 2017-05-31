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

package org.apache.cassandra.cql3.selection;

import java.math.BigDecimal;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.junit.Assert.assertEquals;

public class TermSelectionTest extends CQLTester
{
    // Helper method for testSelectLiteral()
    private void assertConstantResult(UntypedResultSet result, Object constant)
    {
        assertRows(result,
                   row(1, "one", constant),
                   row(2, "two", constant),
                   row(3, "three", constant));
    }

    @Test
    public void testSelectLiteral() throws Throwable
    {
        long timestampInMicros = System.currentTimeMillis() * 1000;
        createTable("CREATE TABLE %s (pk int, ck int, t text, PRIMARY KEY (pk, ck) )");
        execute("INSERT INTO %s (pk, ck, t) VALUES (?, ?, ?) USING TIMESTAMP ?", 1, 1, "one", timestampInMicros);
        execute("INSERT INTO %s (pk, ck, t) VALUES (?, ?, ?) USING TIMESTAMP ?", 1, 2, "two", timestampInMicros);
        execute("INSERT INTO %s (pk, ck, t) VALUES (?, ?, ?) USING TIMESTAMP ?", 1, 3, "three", timestampInMicros);

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, 'a const' FROM %s");
        assertConstantResult(execute("SELECT ck, t, (text)'a const' FROM %s"), "a const");

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, 42 FROM %s");
        assertConstantResult(execute("SELECT ck, t, (smallint)42 FROM %s"), (short) 42);

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, (1, 'foo') FROM %s");
        assertConstantResult(execute("SELECT ck, t, (tuple<int, text>)(1, 'foo') FROM %s"), tuple(1, "foo"));

        assertInvalidMessage("Cannot infer type for term ((1)) in selection clause", "SELECT ck, t, ((1)) FROM %s");
        // We cannot differentiate a tuple containing a tuple from a tuple between parentheses.
        assertInvalidMessage("Cannot infer type for term ((tuple<int>)(1))", "SELECT ck, t, ((tuple<int>)(1)) FROM %s");
        assertConstantResult(execute("SELECT ck, t, (tuple<tuple<int>>)((1)) FROM %s"), tuple(tuple(1)));

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, [1, 2, 3] FROM %s");
        assertConstantResult(execute("SELECT ck, t, (list<int>)[1, 2, 3] FROM %s"), list(1, 2, 3));

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, {1, 2, 3} FROM %s");
        assertConstantResult(execute("SELECT ck, t, (set<int>){1, 2, 3} FROM %s"), set(1, 2, 3));

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, {1: 'foo', 2: 'bar', 3: 'baz'} FROM %s");
        assertConstantResult(execute("SELECT ck, t, (map<int, text>){1: 'foo', 2: 'bar', 3: 'baz'} FROM %s"), map(1, "foo", 2, "bar", 3, "baz"));

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, {} FROM %s");
        assertConstantResult(execute("SELECT ck, t, (map<int, text>){} FROM %s"), map());
        assertConstantResult(execute("SELECT ck, t, (set<int>){} FROM %s"), set());

        assertColumnNames(execute("SELECT ck, t, (int)42, (int)43 FROM %s"), "ck", "t", "(int)42", "(int)43");
        assertRows(execute("SELECT ck, t, (int) 42, (int) 43 FROM %s"),
                   row(1, "one", 42, 43),
                   row(2, "two", 42, 43),
                   row(3, "three", 42, 43));

        assertRows(execute("SELECT min(ck), max(ck), [min(ck), max(ck)] FROM %s"), row(1, 3, list(1, 3)));
        assertRows(execute("SELECT [min(ck), max(ck)] FROM %s"), row(list(1, 3)));
        assertRows(execute("SELECT {min(ck), max(ck)} FROM %s"), row(set(1, 3)));

        // We need to use a cast to differentiate between a map and an UDT
        assertInvalidMessage("Cannot infer type for term {'min': system.min(ck), 'max': system.max(ck)}",
                             "SELECT {'min' : min(ck), 'max' : max(ck)} FROM %s");
        assertRows(execute("SELECT (map<text, int>){'min' : min(ck), 'max' : max(ck)} FROM %s"), row(map("min", 1, "max", 3)));

        assertRows(execute("SELECT [1, min(ck), max(ck)] FROM %s"), row(list(1, 1, 3)));
        assertRows(execute("SELECT {1, min(ck), max(ck)} FROM %s"), row(set(1, 1, 3)));
        assertRows(execute("SELECT (map<text, int>) {'litteral' : 1, 'min' : min(ck), 'max' : max(ck)} FROM %s"), row(map("litteral", 1, "min", 1, "max", 3)));

        // Test List nested within Lists
        assertRows(execute("SELECT [[], [min(ck), max(ck)]] FROM %s"),
                   row(list(list(), list(1, 3))));
        assertRows(execute("SELECT [[], [CAST(pk AS BIGINT), CAST(ck AS BIGINT), WRITETIME(t)]] FROM %s"),
                   row(list(list(), list(1L, 1L, timestampInMicros))),
                   row(list(list(), list(1L, 2L, timestampInMicros))),
                   row(list(list(), list(1L, 3L, timestampInMicros))));
        assertRows(execute("SELECT [[min(ck)], [max(ck)]] FROM %s"),
                   row(list(list(1), list(3))));
        assertRows(execute("SELECT [[min(ck)], ([max(ck)])] FROM %s"),
                   row(list(list(1), list(3))));
        assertRows(execute("SELECT [[pk], [ck]] FROM %s"),
                   row(list(list(1), list(1))),
                   row(list(list(1), list(2))),
                   row(list(list(1), list(3))));
        assertRows(execute("SELECT [[pk], [ck]] FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(list(list(1), list(3))),
                   row(list(list(1), list(2))),
                   row(list(list(1), list(1))));

        // Test Sets nested within Lists
        assertRows(execute("SELECT [{}, {min(ck), max(ck)}] FROM %s"),
                   row(list(set(), set(1, 3))));
        assertRows(execute("SELECT [{}, {CAST(pk AS BIGINT), CAST(ck AS BIGINT), WRITETIME(t)}] FROM %s"),
                   row(list(set(), set(1L, 1L, timestampInMicros))),
                   row(list(set(), set(1L, 2L, timestampInMicros))),
                   row(list(set(), set(1L, 3L, timestampInMicros))));
        assertRows(execute("SELECT [{min(ck)}, {max(ck)}] FROM %s"),
                   row(list(set(1), set(3))));
        assertRows(execute("SELECT [{min(ck)}, ({max(ck)})] FROM %s"),
                   row(list(set(1), set(3))));
        assertRows(execute("SELECT [{pk}, {ck}] FROM %s"),
                   row(list(set(1), set(1))),
                   row(list(set(1), set(2))),
                   row(list(set(1), set(3))));
        assertRows(execute("SELECT [{pk}, {ck}] FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(list(set(1), set(3))),
                   row(list(set(1), set(2))),
                   row(list(set(1), set(1))));

        // Test Maps nested within Lists
        assertRows(execute("SELECT [{}, (map<text, int>){'min' : min(ck), 'max' : max(ck)}] FROM %s"),
                   row(list(map(), map("min", 1, "max", 3))));
        assertRows(execute("SELECT [{}, (map<text, bigint>){'pk' : CAST(pk AS BIGINT), 'ck' : CAST(ck AS BIGINT), 'writetime' : WRITETIME(t)}] FROM %s"),
                   row(list(map(), map("pk", 1L, "ck", 1L, "writetime", timestampInMicros))),
                   row(list(map(), map("pk", 1L, "ck", 2L, "writetime", timestampInMicros))),
                   row(list(map(), map("pk", 1L, "ck", 3L, "writetime", timestampInMicros))));
        assertRows(execute("SELECT [{}, (map<text, int>){'pk' : pk, 'ck' : ck}] FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(list(map(), map("pk", 1, "ck", 3))),
                   row(list(map(), map("pk", 1, "ck", 2))),
                   row(list(map(), map("pk", 1, "ck", 1))));

        // Test Tuples nested within Lists
        assertRows(execute("SELECT [(pk, ck, WRITETIME(t))] FROM %s"),
                   row(list(tuple(1, 1, timestampInMicros))),
                   row(list(tuple(1, 2, timestampInMicros))),
                   row(list(tuple(1, 3, timestampInMicros))));
        assertRows(execute("SELECT [(min(ck), max(ck))] FROM %s"),
                   row(list(tuple(1, 3))));
        assertRows(execute("SELECT [(CAST(pk AS BIGINT), CAST(ck AS BIGINT)), (t, WRITETIME(t))] FROM %s"),
                   row(list(tuple(1L, 1L), tuple("one", timestampInMicros))),
                   row(list(tuple(1L, 2L), tuple("two", timestampInMicros))),
                   row(list(tuple(1L, 3L), tuple("three", timestampInMicros))));

        // Test UDTs nested within Lists
        String type = createType("CREATE TYPE %s(a int, b int, c bigint)");
        assertRows(execute("SELECT [(" + type + "){a : min(ck), b: max(ck)}] FROM %s"),
                   row(list(userType("a", 1, "b", 3, "c", null))));
        assertRows(execute("SELECT [(" + type + "){a : pk, b : ck, c : WRITETIME(t)}] FROM %s"),
                   row(list(userType("a", 1, "b", 1, "c", timestampInMicros))),
                   row(list(userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(list(userType("a", 1, "b", 3, "c", timestampInMicros))));
        assertRows(execute("SELECT [(" + type + "){a : pk, b : ck, c : WRITETIME(t)}] FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(list(userType("a", 1, "b", 3, "c", timestampInMicros))),
                   row(list(userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(list(userType("a", 1, "b", 1, "c", timestampInMicros))));

        // Test Lists nested within Sets
        assertRows(execute("SELECT {[], [min(ck), max(ck)]} FROM %s"),
                   row(set(list(), list(1, 3))));
        assertRows(execute("SELECT {[], [pk, ck]} FROM %s LIMIT 2"),
                   row(set(list(), list(1, 1))),
                   row(set(list(), list(1, 2))));
        assertRows(execute("SELECT {[], [pk, ck]} FROM %s WHERE pk = 1 ORDER BY ck DESC LIMIT 2"),
                   row(set(list(), list(1, 3))),
                   row(set(list(), list(1, 2))));
        assertRows(execute("SELECT {[min(ck)], ([max(ck)])} FROM %s"),
                   row(set(list(1), list(3))));
        assertRows(execute("SELECT {[pk], ([ck])} FROM %s"),
                   row(set(list(1), list(1))),
                   row(set(list(1), list(2))),
                   row(set(list(1), list(3))));
        assertRows(execute("SELECT {([min(ck)]), [max(ck)]} FROM %s"),
                   row(set(list(1), list(3))));

        // Test Sets nested within Sets
        assertRows(execute("SELECT {{}, {min(ck), max(ck)}} FROM %s"),
                   row(set(set(), set(1, 3))));
        assertRows(execute("SELECT {{}, {pk, ck}} FROM %s LIMIT 2"),
                   row(set(set(), set(1, 1))),
                   row(set(set(), set(1, 2))));
        assertRows(execute("SELECT {{}, {pk, ck}} FROM %s WHERE pk = 1 ORDER BY ck DESC LIMIT 2"),
                   row(set(set(), set(1, 3))),
                   row(set(set(), set(1, 2))));
        assertRows(execute("SELECT {{min(ck)}, ({max(ck)})} FROM %s"),
                   row(set(set(1), set(3))));
        assertRows(execute("SELECT {{pk}, ({ck})} FROM %s"),
                   row(set(set(1), set(1))),
                   row(set(set(1), set(2))),
                   row(set(set(1), set(3))));
        assertRows(execute("SELECT {({min(ck)}), {max(ck)}} FROM %s"),
                   row(set(set(1), set(3))));

        // Test Maps nested within Sets
        assertRows(execute("SELECT {{}, (map<text, int>){'min' : min(ck), 'max' : max(ck)}} FROM %s"),
                   row(set(map(), map("min", 1, "max", 3))));
        assertRows(execute("SELECT {{}, (map<text, int>){'pk' : pk, 'ck' : ck}} FROM %s"),
                   row(set(map(), map("pk", 1, "ck", 1))),
                   row(set(map(), map("pk", 1, "ck", 2))),
                   row(set(map(), map("pk", 1, "ck", 3))));

        // Test Tuples nested within Sets
        assertRows(execute("SELECT {(pk, ck, WRITETIME(t))} FROM %s"),
                   row(set(tuple(1, 1, timestampInMicros))),
                   row(set(tuple(1, 2, timestampInMicros))),
                   row(set(tuple(1, 3, timestampInMicros))));
        assertRows(execute("SELECT {(min(ck), max(ck))} FROM %s"),
                   row(set(tuple(1, 3))));

        // Test UDTs nested within Sets
        assertRows(execute("SELECT {(" + type + "){a : min(ck), b: max(ck)}} FROM %s"),
                   row(set(userType("a", 1, "b", 3, "c", null))));
        assertRows(execute("SELECT {(" + type + "){a : pk, b : ck, c : WRITETIME(t)}} FROM %s"),
                   row(set(userType("a", 1, "b", 1, "c", timestampInMicros))),
                   row(set(userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(set(userType("a", 1, "b", 3, "c", timestampInMicros))));
        assertRows(execute("SELECT {(" + type + "){a : pk, b : ck, c : WRITETIME(t)}} FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(set(userType("a", 1, "b", 3, "c", timestampInMicros))),
                   row(set(userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(set(userType("a", 1, "b", 1, "c", timestampInMicros))));

        // Test Lists nested within Maps
        assertRows(execute("SELECT (map<frozen<list<int>>, frozen<list<int>>>){[min(ck)]:[max(ck)]} FROM %s"),
                   row(map(list(1), list(3))));
        assertRows(execute("SELECT (map<frozen<list<int>>, frozen<list<int>>>){[pk]: [ck]} FROM %s"),
                   row(map(list(1), list(1))),
                   row(map(list(1), list(2))),
                   row(map(list(1), list(3))));

        // Test Sets nested within Maps
        assertRows(execute("SELECT (map<frozen<set<int>>, frozen<set<int>>>){{min(ck)} : {max(ck)}} FROM %s"),
                   row(map(set(1), set(3))));
        assertRows(execute("SELECT (map<frozen<set<int>>, frozen<set<int>>>){{pk} : {ck}} FROM %s"),
                   row(map(set(1), set(1))),
                   row(map(set(1), set(2))),
                   row(map(set(1), set(3))));

        // Test Maps nested within Maps
        assertRows(execute("SELECT (map<frozen<map<text, int>>, frozen<map<text, int>>>){{'min' : min(ck)} : {'max' : max(ck)}} FROM %s"),
                   row(map(map("min", 1), map("max", 3))));
        assertRows(execute("SELECT (map<frozen<map<text, int>>, frozen<map<text, int>>>){{'pk' : pk} : {'ck' : ck}} FROM %s"),
                   row(map(map("pk", 1), map("ck", 1))),
                   row(map(map("pk", 1), map("ck", 2))),
                   row(map(map("pk", 1), map("ck", 3))));

        // Test Tuples nested within Maps
        assertRows(execute("SELECT (map<frozen<tuple<int, int>>, frozen<tuple<bigint>>>){(pk, ck) : (WRITETIME(t))} FROM %s"),
                   row(map(tuple(1, 1), tuple(timestampInMicros))),
                   row(map(tuple(1, 2), tuple(timestampInMicros))),
                   row(map(tuple(1, 3), tuple(timestampInMicros))));
        assertRows(execute("SELECT (map<frozen<tuple<int>> , frozen<tuple<int>>>){(min(ck)) : (max(ck))} FROM %s"),
                   row(map(tuple(1), tuple(3))));

        // Test UDTs nested within Maps
        assertRows(execute("SELECT (map<int, frozen<" + type + ">>){ck : {a : min(ck), b: max(ck)}} FROM %s"),
                   row(map(1, userType("a", 1, "b", 3, "c", null))));
        assertRows(execute("SELECT (map<int, frozen<" + type + ">>){ck : {a : pk, b : ck, c : WRITETIME(t)}} FROM %s"),
                   row(map(1, userType("a", 1, "b", 1, "c", timestampInMicros))),
                   row(map(2, userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(map(3, userType("a", 1, "b", 3, "c", timestampInMicros))));
        assertRows(execute("SELECT (map<int, frozen<" + type + ">>){ck : {a : pk, b : ck, c : WRITETIME(t)}} FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(map(3, userType("a", 1, "b", 3, "c", timestampInMicros))),
                   row(map(2, userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(map(1, userType("a", 1, "b", 1, "c", timestampInMicros))));

        // Test Lists nested within Tuples
        assertRows(execute("SELECT ([min(ck)], [max(ck)]) FROM %s"),
                   row(tuple(list(1), list(3))));
        assertRows(execute("SELECT ([pk], [ck]) FROM %s"),
                   row(tuple(list(1), list(1))),
                   row(tuple(list(1), list(2))),
                   row(tuple(list(1), list(3))));

        // Test Sets nested within Tuples
        assertRows(execute("SELECT ({min(ck)}, {max(ck)}) FROM %s"),
                   row(tuple(set(1), set(3))));
        assertRows(execute("SELECT ({pk}, {ck}) FROM %s"),
                   row(tuple(set(1), set(1))),
                   row(tuple(set(1), set(2))),
                   row(tuple(set(1), set(3))));

        // Test Maps nested within Tuples
        assertRows(execute("SELECT ((map<text, int>){'min' : min(ck)}, (map<text, int>){'max' : max(ck)}) FROM %s"),
                   row(tuple(map("min", 1), map("max", 3))));
        assertRows(execute("SELECT ((map<text, int>){'pk' : pk}, (map<text, int>){'ck' : ck}) FROM %s"),
                   row(tuple(map("pk", 1), map("ck", 1))),
                   row(tuple(map("pk", 1), map("ck", 2))),
                   row(tuple(map("pk", 1), map("ck", 3))));

        // Test Tuples nested within Tuples
        assertRows(execute("SELECT (tuple<tuple<int, int, bigint>>)((pk, ck, WRITETIME(t))) FROM %s"),
                   row(tuple(tuple(1, 1, timestampInMicros))),
                   row(tuple(tuple(1, 2, timestampInMicros))),
                   row(tuple(tuple(1, 3, timestampInMicros))));
        assertRows(execute("SELECT (tuple<tuple<int, int, bigint>>)((min(ck), max(ck))) FROM %s"),
                   row(tuple(tuple(1, 3))));

        assertRows(execute("SELECT ((t, WRITETIME(t)), (CAST(pk AS BIGINT), CAST(ck AS BIGINT))) FROM %s"),
                   row(tuple(tuple("one", timestampInMicros), tuple(1L, 1L))),
                   row(tuple(tuple("two", timestampInMicros), tuple(1L, 2L))),
                   row(tuple(tuple("three", timestampInMicros), tuple(1L, 3L))));

        // Test UDTs nested within Tuples
        assertRows(execute("SELECT (tuple<" + type + ">)({a : min(ck), b: max(ck)}) FROM %s"),
                   row(tuple(userType("a", 1, "b", 3, "c", null))));
        assertRows(execute("SELECT (tuple<" + type + ">)({a : pk, b : ck, c : WRITETIME(t)}) FROM %s"),
                   row(tuple(userType("a", 1, "b", 1, "c", timestampInMicros))),
                   row(tuple(userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(tuple(userType("a", 1, "b", 3, "c", timestampInMicros))));
        assertRows(execute("SELECT (tuple<" + type + ">)({a : pk, b : ck, c : WRITETIME(t)}) FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(tuple(userType("a", 1, "b", 3, "c", timestampInMicros))),
                   row(tuple(userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(tuple(userType("a", 1, "b", 1, "c", timestampInMicros))));

        // Test Lists nested within UDTs
        String containerType = createType("CREATE TYPE %s(l list<int>)");
        assertRows(execute("SELECT (" + containerType + "){l : [min(ck), max(ck)]} FROM %s"),
                   row(userType("l", list(1, 3))));
        assertRows(execute("SELECT (" + containerType + "){l : [pk, ck]} FROM %s"),
                   row(userType("l", list(1, 1))),
                   row(userType("l", list(1, 2))),
                   row(userType("l", list(1, 3))));

        // Test Sets nested within UDTs
        containerType = createType("CREATE TYPE %s(s set<int>)");
        assertRows(execute("SELECT (" + containerType + "){s : {min(ck), max(ck)}} FROM %s"),
                   row(userType("s", set(1, 3))));
        assertRows(execute("SELECT (" + containerType + "){s : {pk, ck}} FROM %s"),
                   row(userType("s", set(1))),
                   row(userType("s", set(1, 2))),
                   row(userType("s", set(1, 3))));

        // Test Maps nested within UDTs
        containerType = createType("CREATE TYPE %s(m map<text, int>)");
        assertRows(execute("SELECT (" + containerType + "){m : {'min' : min(ck), 'max' : max(ck)}} FROM %s"),
                   row(userType("m", map("min", 1, "max", 3))));
        assertRows(execute("SELECT (" + containerType + "){m : {'pk' : pk, 'ck' : ck}} FROM %s"),
                   row(userType("m", map("pk", 1, "ck", 1))),
                   row(userType("m", map("pk", 1, "ck", 2))),
                   row(userType("m", map("pk", 1, "ck", 3))));

        // Test Tuples nested within UDTs
        containerType = createType("CREATE TYPE %s(t tuple<int, int>, w tuple<bigint>)");
        assertRows(execute("SELECT (" + containerType + "){t : (pk, ck), w : (WRITETIME(t))} FROM %s"),
                   row(userType("t", tuple(1, 1), "w", tuple(timestampInMicros))),
                   row(userType("t", tuple(1, 2), "w", tuple(timestampInMicros))),
                   row(userType("t", tuple(1, 3), "w", tuple(timestampInMicros))));

        // Test UDTs nested within Maps
        containerType = createType("CREATE TYPE %s(t frozen<" + type + ">)");
        assertRows(execute("SELECT (" + containerType + "){t : {a : min(ck), b: max(ck)}} FROM %s"),
                   row(userType("t", userType("a", 1, "b", 3, "c", null))));
        assertRows(execute("SELECT (" + containerType + "){t : {a : pk, b : ck, c : WRITETIME(t)}} FROM %s"),
                   row(userType("t", userType("a", 1, "b", 1, "c", timestampInMicros))),
                   row(userType("t", userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(userType("t", userType("a", 1, "b", 3, "c", timestampInMicros))));
        assertRows(execute("SELECT (" + containerType + "){t : {a : pk, b : ck, c : WRITETIME(t)}} FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(userType("t", userType("a", 1, "b", 3, "c", timestampInMicros))),
                   row(userType("t", userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(userType("t", userType("a", 1, "b", 1, "c", timestampInMicros))));


        // Test Litteral Set with Duration elements
        assertInvalidMessage("Durations are not allowed inside sets: set<duration>",
                             "SELECT pk, ck, (set<duration>){2d, 1mo} FROM %s");

        assertInvalidMessage("Invalid field selection: system.min(ck) of type int is not a user type",
                             "SELECT min(ck).min FROM %s");
        assertInvalidMessage("Invalid field selection: (map<text, int>){'min': system.min(ck), 'max': system.max(ck)} of type frozen<map<text, int>> is not a user type",
                             "SELECT (map<text, int>) {'min' : min(ck), 'max' : max(ck)}.min FROM %s");
    }

    @Test
    public void testCollectionLiteralsWithDurations() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, d1 duration, d2 duration, PRIMARY KEY (pk, ck) )");
        execute("INSERT INTO %s (pk, ck, d1, d2) VALUES (1, 1, 15h, 13h)");
        execute("INSERT INTO %s (pk, ck, d1, d2) VALUES (1, 2, 10h, 12h)");
        execute("INSERT INTO %s (pk, ck, d1, d2) VALUES (1, 3, 11h, 13h)");

        assertRows(execute("SELECT [d1, d2] FROM %s"),
                   row(list(Duration.from("15h"), Duration.from("13h"))),
                   row(list(Duration.from("10h"), Duration.from("12h"))),
                   row(list(Duration.from("11h"), Duration.from("13h"))));

        assertInvalidMessage("Durations are not allowed inside sets: frozen<set<duration>>", "SELECT {d1, d2} FROM %s");

        assertRows(execute("SELECT (map<int, duration>){ck : d1} FROM %s"),
                   row(map(1, Duration.from("15h"))),
                   row(map(2, Duration.from("10h"))),
                   row(map(3, Duration.from("11h"))));

        assertInvalidMessage("Durations are not allowed as map keys: map<duration, int>",
                             "SELECT (map<duration, int>){d1 : ck, d2 :ck} FROM %s");
    }

    @Test
    public void testSelectUDTLiteral() throws Throwable
    {
        String type = createType("CREATE TYPE %s(a int, b text)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + type + ")");

        execute("INSERT INTO %s(k, v) VALUES (?, ?)", 0, userType("a", 3, "b", "foo"));

        assertInvalidMessage("Cannot infer type for term", "SELECT { a: 4, b: 'bar'} FROM %s");

        assertRows(execute("SELECT k, v, (" + type + "){ a: 4, b: 'bar'} FROM %s"),
            row(0, userType("a", 3, "b", "foo"), userType("a", 4, "b", "bar"))
        );

        assertRows(execute("SELECT k, v, (" + type + ")({ a: 4, b: 'bar'}) FROM %s"),
            row(0, userType("a", 3, "b", "foo"), userType("a", 4, "b", "bar"))
        );

        assertRows(execute("SELECT k, v, ((" + type + "){ a: 4, b: 'bar'}).a FROM %s"),
                   row(0, userType("a", 3, "b", "foo"), 4)
        );

        assertRows(execute("SELECT k, v, (" + type + "){ a: 4, b: 'bar'}.a FROM %s"),
                   row(0, userType("a", 3, "b", "foo"), 4)
        );

        assertInvalidMessage("Cannot infer type for term", "SELECT { a: 4} FROM %s");

        assertRows(execute("SELECT k, v, (" + type + "){ a: 4} FROM %s"),
            row(0, userType("a", 3, "b", "foo"), userType("a", 4, "b", null))
        );

        assertRows(execute("SELECT k, v, (" + type + "){ b: 'bar'} FROM %s"),
                   row(0, userType("a", 3, "b", "foo"), userType("a", null, "b", "bar"))
        );

        execute("INSERT INTO %s(k, v) VALUES (?, ?)", 1, userType("a", 5, "b", "foo"));
        assertRows(execute("SELECT (" + type + "){ a: max(v.a) , b: 'max'} FROM %s"),
                   row(userType("a", 5, "b", "max"))
        );
        assertRows(execute("SELECT (" + type + "){ a: min(v.a) , b: 'min'} FROM %s"),
                   row(userType("a", 3, "b", "min"))
        );
    }

    @Test
    public void testInvalidSelect() throws Throwable
    {
        // Creates a table just so we can reference it in the (invalid) SELECT below
        createTable("CREATE TABLE %s (k int PRIMARY KEY)");

        assertInvalidMessage("Cannot infer type for term", "SELECT ? FROM %s");
        assertInvalidMessage("Cannot infer type for term", "SELECT k, ? FROM %s");

        assertInvalidMessage("Cannot infer type for term", "SELECT k, null FROM %s");
    }

    private void assertColumnSpec(ColumnSpecification spec, String expectedName, AbstractType<?> expectedType)
    {
        assertEquals(expectedName, spec.name.toString());
        assertEquals(expectedType, spec.type);
    }

    @Test
    public void testSelectPrepared() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, t text, PRIMARY KEY (pk, ck) )");
        execute("INSERT INTO %s (pk, ck, t) VALUES (1, 1, 'one')");
        execute("INSERT INTO %s (pk, ck, t) VALUES (1, 2, 'two')");
        execute("INSERT INTO %s (pk, ck, t) VALUES (1, 3, 'three')");

        String query = "SELECT (int)?, (decimal):adecimal, (text)?, (tuple<int,text>):atuple, pk, ck, t FROM %s WHERE pk = ?";
        ResultMessage.Prepared prepared = prepare(query);

        List<ColumnSpecification> boundNames = prepared.metadata.names;

        // 5 bound variables
        assertEquals(5, boundNames.size());
        assertColumnSpec(boundNames.get(0), "[selection]", Int32Type.instance);
        assertColumnSpec(boundNames.get(1), "adecimal", DecimalType.instance);
        assertColumnSpec(boundNames.get(2), "[selection]", UTF8Type.instance);
        assertColumnSpec(boundNames.get(3), "atuple", TypeParser.parse("TupleType(Int32Type,UTF8Type)"));
        assertColumnSpec(boundNames.get(4), "pk", Int32Type.instance);


        List<ColumnSpecification> resultNames = prepared.resultMetadata.names;

        // 7 result "columns"
        assertEquals(7, resultNames.size());
        assertColumnSpec(resultNames.get(0), "(int)?", Int32Type.instance);
        assertColumnSpec(resultNames.get(1), "(decimal)?", DecimalType.instance);
        assertColumnSpec(resultNames.get(2), "(text)?", UTF8Type.instance);
        assertColumnSpec(resultNames.get(3), "(tuple<int, text>)?", TypeParser.parse("TupleType(Int32Type,UTF8Type)"));
        assertColumnSpec(resultNames.get(4), "pk", Int32Type.instance);
        assertColumnSpec(resultNames.get(5), "ck", Int32Type.instance);
        assertColumnSpec(resultNames.get(6), "t", UTF8Type.instance);

        assertRows(execute(query, 88, BigDecimal.TEN, "foo bar baz", tuple(42, "ursus"), 1),
                   row(88, BigDecimal.TEN, "foo bar baz", tuple(42, "ursus"),
                       1, 1, "one"),
                   row(88, BigDecimal.TEN, "foo bar baz", tuple(42, "ursus"),
                       1, 2, "two"),
                   row(88, BigDecimal.TEN, "foo bar baz", tuple(42, "ursus"),
                       1, 3, "three"));
    }

    @Test
    public void testConstantFunctionArgs() throws Throwable
    {
        String fInt = createFunction(KEYSPACE,
                                     "int,int",
                                     "CREATE FUNCTION %s (val1 int, val2 int) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS int " +
                                     "LANGUAGE java\n" +
                                     "AS 'return Math.max(val1, val2);';");
        String fFloat = createFunction(KEYSPACE,
                                       "float,float",
                                       "CREATE FUNCTION %s (val1 float, val2 float) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS float " +
                                       "LANGUAGE java\n" +
                                       "AS 'return Math.max(val1, val2);';");
        String fText = createFunction(KEYSPACE,
                                      "text,text",
                                      "CREATE FUNCTION %s (val1 text, val2 text) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE java\n" +
                                      "AS 'return val2;';");
        String fAscii = createFunction(KEYSPACE,
                                       "ascii,ascii",
                                       "CREATE FUNCTION %s (val1 ascii, val2 ascii) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS ascii " +
                                       "LANGUAGE java\n" +
                                       "AS 'return val2;';");
        String fTimeuuid = createFunction(KEYSPACE,
                                          "timeuuid,timeuuid",
                                          "CREATE FUNCTION %s (val1 timeuuid, val2 timeuuid) " +
                                          "CALLED ON NULL INPUT " +
                                          "RETURNS timeuuid " +
                                          "LANGUAGE java\n" +
                                          "AS 'return val2;';");

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, valInt int, valFloat float, valText text, valAscii ascii, valTimeuuid timeuuid)");
        execute("INSERT INTO %s (pk, valInt, valFloat, valText, valAscii, valTimeuuid) " +
                "VALUES (1, 10, 10.0, '100', '100', 2deb23e0-96b5-11e5-b26d-a939dd1405a3)");

        assertRows(execute("SELECT pk, " + fInt + "(valInt, 100) FROM %s"),
                   row(1, 100));
        assertRows(execute("SELECT pk, " + fInt + "(valInt, (int)100) FROM %s"),
                   row(1, 100));
        assertInvalidMessage("Type error: (bigint)100 cannot be passed as argument 1 of function",
                             "SELECT pk, " + fInt + "(valInt, (bigint)100) FROM %s");
        assertRows(execute("SELECT pk, " + fFloat + "(valFloat, (float)100.00) FROM %s"),
                   row(1, 100f));
        assertRows(execute("SELECT pk, " + fText + "(valText, 'foo') FROM %s"),
                   row(1, "foo"));
        assertRows(execute("SELECT pk, " + fAscii + "(valAscii, (ascii)'foo') FROM %s"),
                   row(1, "foo"));
        assertRows(execute("SELECT pk, " + fTimeuuid + "(valTimeuuid, (timeuuid)34617f80-96b5-11e5-b26d-a939dd1405a3) FROM %s"),
                   row(1, UUID.fromString("34617f80-96b5-11e5-b26d-a939dd1405a3")));

        // ambiguous

        String fAmbiguousFunc1 = createFunction(KEYSPACE,
                                                "int,bigint",
                                                "CREATE FUNCTION %s (val1 int, val2 bigint) " +
                                                "CALLED ON NULL INPUT " +
                                                "RETURNS bigint " +
                                                "LANGUAGE java\n" +
                                                "AS 'return Math.max((long)val1, val2);';");
        assertRows(execute("SELECT pk, " + fAmbiguousFunc1 + "(valInt, 100) FROM %s"),
                   row(1, 100L));
        createFunctionOverload(fAmbiguousFunc1, "int,int",
                                                "CREATE FUNCTION %s (val1 int, val2 int) " +
                                                "CALLED ON NULL INPUT " +
                                                "RETURNS int " +
                                                "LANGUAGE java\n" +
                                                "AS 'return Math.max(val1, val2);';");
        assertRows(execute("SELECT pk, " + fAmbiguousFunc1 + "(valInt, 100) FROM %s"),
                   row(1, 100));
    }

    @Test
    public void testPreparedFunctionArgs() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, t text, i int, PRIMARY KEY (pk, ck) )");
        execute("INSERT INTO %s (pk, ck, t, i) VALUES (1, 1, 'one', 50)");
        execute("INSERT INTO %s (pk, ck, t, i) VALUES (1, 2, 'two', 100)");
        execute("INSERT INTO %s (pk, ck, t, i) VALUES (1, 3, 'three', 150)");

        String fIntMax = createFunction(KEYSPACE,
                                        "int,int",
                                        "CREATE FUNCTION %s (val1 int, val2 int) " +
                                        "CALLED ON NULL INPUT " +
                                        "RETURNS int " +
                                        "LANGUAGE java\n" +
                                        "AS 'return Math.max(val1, val2);';");

        // weak typing

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s", 0),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s", 100),
                   row(1, 1, 100),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s", 200),
                   row(1, 1, 200),
                   row(1, 2, 200),
                   row(1, 3, 200));

        // explicit typing

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s", 0),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s", 100),
                   row(1, 1, 100),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s", 200),
                   row(1, 1, 200),
                   row(1, 2, 200),
                   row(1, 3, 200));

        // weak typing

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s WHERE pk = " + fIntMax + "(1,1)", 0),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s WHERE pk = " + fIntMax + "(2,1)", 0));

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s WHERE pk = " + fIntMax + "(?,1)", 0, 1),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s WHERE pk = " + fIntMax + "(?,1)", 0, 2));

        // explicit typing

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)1,(int)1)", 0),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)2,(int)1)", 0));

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)?,(int)1)", 0, 1),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)?,(int)1)", 0, 2));

        assertInvalidMessage("Invalid unset value for argument", "SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)1,(int)1)", unset());
    }

    @Test
    public void testInsertUpdateDelete() throws Throwable
    {
        String fIntMax = createFunction(KEYSPACE,
                                        "int,int",
                                        "CREATE FUNCTION %s (val1 int, val2 int) " +
                                        "CALLED ON NULL INPUT " +
                                        "RETURNS int " +
                                        "LANGUAGE java\n" +
                                        "AS 'return Math.max(val1, val2);';");

        createTable("CREATE TABLE %s (pk int, ck int, t text, i int, PRIMARY KEY (pk, ck) )");

        execute("UPDATE %s SET i = " + fIntMax + "(100, 200) WHERE pk = 1 AND ck = 1");
        assertRows(execute("SELECT i FROM %s WHERE pk = 1 AND ck = 1"),
                   row(200));

        execute("UPDATE %s SET i = " + fIntMax + "(100, 300) WHERE pk = 1 AND ck = " + fIntMax + "(1,2)");
        assertRows(execute("SELECT i FROM %s WHERE pk = 1 AND ck = 2"),
                   row(300));

        execute("DELETE FROM %s WHERE pk = 1 AND ck = " + fIntMax + "(1,2)");
        assertRows(execute("SELECT i FROM %s WHERE pk = 1 AND ck = 2"));

        execute("INSERT INTO %s (pk, ck, i) VALUES (1, " + fIntMax + "(1,2), " + fIntMax + "(100, 300))");
        assertRows(execute("SELECT i FROM %s WHERE pk = 1 AND ck = 2"),
                   row(300));
    }
}
