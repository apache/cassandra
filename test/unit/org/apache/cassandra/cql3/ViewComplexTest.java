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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

import static org.junit.Assert.fail;

/* ViewComplexTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670, CASSANDRA-17167)
 * Any changes here check if they apply to the other classes:
 * - ViewComplexUpdatesTest
 * - ViewComplexDeletionsTest
 * - ViewComplexTTLTest
 * - ViewComplexTest
 * - ViewComplexLivenessTest
 * - ...
 * - ViewComplex*Test
 */
public class ViewComplexTest extends ViewAbstractParameterizedTest
{
    @Test
    public void testNonBaseColumnInViewPkWithFlush() throws Throwable
    {
        testNonBaseColumnInViewPk(true);
    }

    @Test
    public void testNonBaseColumnInViewPkWithoutFlush() throws Throwable
    {
        testNonBaseColumnInViewPk(false);
    }

    private void testNonBaseColumnInViewPk(boolean flush) throws Throwable
    {
        createTable("create table %s (p1 int, p2 int, v1 int, v2 int, primary key (p1,p2))");

        Keyspace ks = Keyspace.open(keyspace());

        createView("create materialized view %s as select * from %s " +
                   "where p1 is not null and p2 is not null primary key (p2, p1) " +
                   "with gc_grace_seconds=5");
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(currentView());
        cfs.disableAutoCompaction();

        updateView("UPDATE %s USING TIMESTAMP 1 set v1 =1 where p1 = 1 AND p2 = 1;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"), row(1, 1, 1, null));
        assertRowsIgnoringOrder(executeView("SELECT p1, p2, v1, v2 from %s"), row(1, 1, 1, null));

        updateView("UPDATE %s USING TIMESTAMP 2 set v1 = null, v2 = 1 where p1 = 1 AND p2 = 1;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"), row(1, 1, null, 1));
        assertRowsIgnoringOrder(executeView("SELECT p1, p2, v1, v2 from %s"), row(1, 1, null, 1));

        updateView("UPDATE %s USING TIMESTAMP 2 set v2 = null where p1 = 1 AND p2 = 1;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"));
        assertRowsIgnoringOrder(executeView("SELECT p1, p2, v1, v2 from %s"));

        updateView("INSERT INTO %s (p1,p2) VALUES(1,1) USING TIMESTAMP 3;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"), row(1, 1, null, null));
        assertRowsIgnoringOrder(executeView("SELECT p1, p2, v1, v2 from %s"), row(1, 1, null, null));

        updateView("DELETE FROM %s USING TIMESTAMP 4 WHERE p1 =1 AND p2 = 1;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"));
        assertRowsIgnoringOrder(executeView("SELECT p1, p2, v1, v2 from %s"));

        updateView("UPDATE %s USING TIMESTAMP 5 set v2 = 1 where p1 = 1 AND p2 = 1;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"), row(1, 1, null, 1));
        assertRowsIgnoringOrder(executeView("SELECT p1, p2, v1, v2 from %s"), row(1, 1, null, 1));
    }

    @Test
    public void testMVWithDifferentColumnsWithFlush() throws Throwable
    {
        testMVWithDifferentColumns(true);
    }

    @Test
    public void testMVWithDifferentColumnsWithoutFlush() throws Throwable
    {
        testMVWithDifferentColumns(false);
    }

    private void testMVWithDifferentColumns(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, f int, PRIMARY KEY(a, b))");

        List<String> viewNames = new ArrayList<>();
        List<String> mvStatements = Arrays.asList(
                                                  // all selected
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a,b)",
                                                  // unselected e,f
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT a,b,c,d FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a,b)",
                                                  // no selected
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a,b)",
                                                  // all selected, re-order keys
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)",
                                                  // unselected e,f, re-order keys
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT a,b,c,d FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)",
                                                  // no selected, re-order keys
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)");

        Keyspace ks = Keyspace.open(keyspace());

        for (int i = 0; i < mvStatements.size(); i++)
        {
            String name = createView(mvStatements.get(i));
            viewNames.add(name);
            ks.getColumnFamilyStore(name).disableAutoCompaction();
        }

        // insert
        updateViewWithFlush("INSERT INTO %s (a,b,c,d,e,f) VALUES(1,1,1,1,1,1) using timestamp 1", flush);
        assertBaseViews(row(1, 1, 1, 1, 1, 1), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 2 SET c=0, d=0 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, 0, 0, 1, 1), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 2 SET e=0, f=0 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, 0, 0, 0, 0), viewNames);

        updateViewWithFlush("DELETE FROM %s using timestamp 2 WHERE a=1 AND b=1", flush);
        assertBaseViews(null, viewNames);

        // partial update unselected, selected
        updateViewWithFlush("UPDATE %s using timestamp 3 SET f=1 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, null, null, null, 1), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 4 SET e = 1, f=null WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, null, null, 1, null), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 4 SET e = null WHERE a=1 AND b=1", flush);
        assertBaseViews(null, viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 5 SET c = 1 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, 1, null, null, null), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 5 SET c = null WHERE a=1 AND b=1", flush);
        assertBaseViews(null, viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 6 SET d = 1 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, null, 1, null, null), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 7 SET d = null WHERE a=1 AND b=1", flush);
        assertBaseViews(null, viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 8 SET f = 1 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, null, null, null, 1), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 6 SET c = 1 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, 1, null, null, 1), viewNames);

        // view row still alive due to c=1@6
        updateViewWithFlush("UPDATE %s using timestamp 8 SET f = null WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, 1, null, null, null), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 6 SET c = null WHERE a=1 AND b=1", flush);
        assertBaseViews(null, viewNames);
    }

    private void assertBaseViews(Object[] row, List<String> viewNames) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s");
        if (row == null)
            assertRowsIgnoringOrder(result);
        else
            assertRowsIgnoringOrder(result, row);
        for (int i = 0; i < viewNames.size(); i++)
            assertBaseView(result, execute(String.format("SELECT * FROM %s", viewNames.get(i))), viewNames.get(i));
    }

    private void assertBaseView(UntypedResultSet base, UntypedResultSet view, String mv)
    {
        List<ColumnSpecification> baseMeta = base.metadata();
        List<ColumnSpecification> viewMeta = view.metadata();

        Iterator<UntypedResultSet.Row> iter = base.iterator();
        Iterator<UntypedResultSet.Row> viewIter = view.iterator();

        List<UntypedResultSet.Row> baseData = com.google.common.collect.Lists.newArrayList(iter);
        List<UntypedResultSet.Row> viewData = com.google.common.collect.Lists.newArrayList(viewIter);

        if (baseData.size() != viewData.size())
            fail(String.format("Mismatch number of rows in view %s: <%s>, in base <%s>",
                               mv,
                               makeRowStrings(view),
                               makeRowStrings(base)));
        if (baseData.size() == 0)
            return;
        if (viewData.size() != 1)
            fail(String.format("Expect only one row in view %s, but got <%s>",
                               mv,
                               makeRowStrings(view)));

        UntypedResultSet.Row row = baseData.get(0);
        UntypedResultSet.Row viewRow = viewData.get(0);

        Map<String, ByteBuffer> baseValues = new HashMap<>();
        for (int j = 0; j < baseMeta.size(); j++)
        {
            ColumnSpecification column = baseMeta.get(j);
            ByteBuffer actualValue = row.getBytes(column.name.toString());
            baseValues.put(column.name.toString(), actualValue);
        }
        for (int j = 0; j < viewMeta.size(); j++)
        {
            ColumnSpecification column = viewMeta.get(j);
            String name = column.name.toString();
            ByteBuffer viewValue = viewRow.getBytes(name);
            if (!baseValues.containsKey(name))
            {
                fail(String.format("Extra column: %s with value %s in view", name, column.type.compose(viewValue)));
            }
            else if (!Objects.equal(baseValues.get(name), viewValue))
            {
                fail(String.format("Non equal column: %s, expected <%s> but got <%s>",
                                   name,
                                   column.type.compose(baseValues.get(name)),
                                   column.type.compose(viewValue)));
            }
        }
    }
}
