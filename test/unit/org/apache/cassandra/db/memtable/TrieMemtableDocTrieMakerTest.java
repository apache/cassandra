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
package org.apache.cassandra.db.memtable;

import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.TrieToDot;
import org.apache.cassandra.db.tries.TrieToMermaid;

public class TrieMemtableDocTrieMakerTest extends CQLTester
{
    @Test
    public void makeTrieDumps() throws Throwable
    {
        createTable("CREATE TABLE %s (company text, date date, total double, purchases map<uuid, double>, PRIMARY KEY (company, date)) WITH memtable = 'trie';");

        execute("INSERT INTO %s (company, date, total, purchases) values (?, '2026-02-10', ?, ?) using timestamp 346",
                "ACME",
                256.68,
                Map.of(UUID.fromString("d79012af-8b34-4fb4-9799-6c0d29ca4e2f"), 88.67,
                       UUID.fromString("830b82ce-a7f2-4939-9ea1-46b9d3714848"), 256.68 - 88.67));

        execute("INSERT INTO %s (company, date, total) values (?, '2026-02-13', ?) using timestamp 385", "ACME", 99.23);
        execute("UPDATE %s using timestamp 385 SET purchases[?] = ? WHERE company = ? AND date = '2026-02-13'",
                UUID.fromString("dab4819d-c6f5-4c05-b575-4a057d78c99a"),
                99.23,
                "ACME");

        execute("INSERT INTO %s (company, date, total, purchases) values (?, '2026-02-11', ?, ?) using timestamp 352",
                "IBM",
                542.79,
                Map.of(UUID.fromString("3edf143a-a8d1-478f-86b4-d83a72a75170"), 122.12,
                       UUID.fromString("35441ee9-3ac9-40d0-98e8-9c75b414addb"), 542.79 - 122.12));

        execute("INSERT INTO %s (company, date, total, purchases) values (?, '2026-02-12', ?, ?) using timestamp 367",
                "Apple",
                324.83,
                Map.of(UUID.fromString("82b4ce57-d6a0-4470-8747-1c2aa4fc5961"), 324.83));
        execute("DELETE FROM %s using timestamp 329 WHERE company = 'Apple' AND date = '2026-02-09'");
        execute("DELETE FROM %s using timestamp 412 where company='Apple' AND date<='2026-01-31' AND date>='2026-01-01'");

        TrieMemtable memtable = (TrieMemtable) getCurrentColumnFamilyStore().getCurrentMemtable();
        System.out.println();
        System.out.println(toShortString(memtable.dump()));
        System.out.println();
        System.out.println(memtable.mergedTrie.dump(TrieMemtableDocTrieMakerTest::toShortString, TrieMemtableDocTrieMakerTest::toShortString));
        System.out.println();
        System.out.println(memtable.mergedTrie.process(Direction.FORWARD, new TrieToMermaid<>(TrieMemtableDocTrieMakerTest::toShortString, TrieMemtableDocTrieMakerTest::toShortString, x -> String.format("%02x", x), true)));
        System.out.println();
        System.out.println(memtable.mergedTrie.process(Direction.FORWARD, new TrieToDot<>(TrieMemtableDocTrieMakerTest::toShortString, TrieMemtableDocTrieMakerTest::toShortString, x -> String.format("%02x", x), true)));
        System.out.println();
    }

    public static <T> String toShortString(T val)
    {
        String v = val.toString();
        return v.replaceAll(", localDeletion=\\d+", "");//.replaceAll("(ts|deletedAt)=\\d+(\\d{3})","$1=$2");
    }
}
