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

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.SAITester;

/**
* A CONTAINS / CONTAINS KEY restriction (ALLOW FILTERING) on an unindexed frozen collection column must return the
* same rows whether or not the query is driven by an SAI index on another column.
* <p>
* Every test uses the same three rows and expects only {@code k = 1}:
* <pre>
*   k=0: c = A, v = 1   (v matches, c does not)
*   k=1: c = B, v = 1   (v matches, c matches)   &lt;-- expected
*   k=2: c = B, v = 2   (c matches, v does not)
* </pre>
* The {@code sai*} tests use an SAI index on {@code v}; the rest are cases that already return the right rows (no index, legacy 2i on {@code v},
* or a non-frozen collection). Equality ({@code c = B}) cases are included to show the whole-value comparison works.
*/
@RunWith(Parameterized.class)
public class FrozenCollectionUnindexedContainsTest extends SAITester
{
   @Parameterized.Parameter(0)
   public String type;        // collection type, without frozen<>
   @Parameterized.Parameter(1)
   public String nonMatching; // literal for k=0
   @Parameterized.Parameter(2)
   public String matching;    // literal for k=1 and k=2
   @Parameterized.Parameter(3)
   public String restriction; // restriction on c that matches only `matching`
   @Parameterized.Parameter(4)
   public boolean flush;

   @Parameterized.Parameters(name = "{0} {3} flush={4}")
   public static Collection<Object[]> data()
   {
       Object[][] cases = {
           { "map<int, int>", "{1: 10}", "{2: 20}", "c CONTAINS KEY 2" },
           { "map<int, int>", "{1: 10}", "{2: 20}", "c CONTAINS 20" },
           { "set<int>",      "{1}",     "{2}",     "c CONTAINS 2" },
           { "list<int>",     "[1]",     "[2]",     "c CONTAINS 2" },
           { "map<int, int>", "{1: 10}", "{2: 20}", "c = {2: 20}" },
           { "set<int>",      "{1}",     "{2}",     "c = {2}" },
           { "list<int>",     "[1]",     "[2]",     "c = [2]" },
       };
       List<Object[]> result = new ArrayList<>();
       for (boolean flush : new boolean[]{ false, true })
           for (Object[] c : cases)
               result.add(new Object[]{ c[0], c[1], c[2], c[3], flush });
       return result;
   }

   // ---- SAI on v, frozen collection: suspected bug ----

   @Test
   public void saiFrozenRegular() throws Throwable
   {
       test(regularTable(true), "sai");
   }

   @Test
   public void saiFrozenClustering() throws Throwable
   {
       test(clusteringTable(), "sai");
   }

   // ---- cases that return the right rows with or without the fix ----

   @Test
   public void noIndexFrozenRegular() throws Throwable
   {
       test(regularTable(true), null);
   }

   @Test
   public void noIndexFrozenClustering() throws Throwable
   {
       test(clusteringTable(), null);
   }

   @Test
   public void legacyIndexFrozenRegular() throws Throwable
   {
       test(regularTable(true), "legacy_local_table");
   }

   @Test
   public void legacyIndexFrozenClustering() throws Throwable
   {
       test(clusteringTable(), "legacy_local_table");
   }

   @Test
   public void saiNonFrozenRegular() throws Throwable
   {
       Assume.assumeTrue(restriction.contains("CONTAINS"));
       test(regularTable(false), "sai");
   }

   private String regularTable(boolean frozen)
   {
       String colType = frozen ? "frozen<" + type + '>' : type;
       return "CREATE TABLE %s (k int PRIMARY KEY, c " + colType + ", v int)";
   }

   private String clusteringTable()
   {
       return "CREATE TABLE %s (k int, c frozen<" + type + ">, v int, PRIMARY KEY (k, c))";
   }

   private void test(String ddl, String indexType) throws Throwable
   {
       createTable(ddl);
       if (indexType != null)
           createIndex("CREATE INDEX ON %s(v) USING '" + indexType + '\'');

       execute("INSERT INTO %s (k, c, v) VALUES (0, " + nonMatching + ", 1)");
       execute("INSERT INTO %s (k, c, v) VALUES (1, " + matching + ", 1)");
       execute("INSERT INTO %s (k, c, v) VALUES (2, " + matching + ", 2)");
       if (flush)
           flush();

       assertRows(execute("SELECT k FROM %s WHERE v = 1 AND " + restriction + " ALLOW FILTERING"), row(1));
   }
}
