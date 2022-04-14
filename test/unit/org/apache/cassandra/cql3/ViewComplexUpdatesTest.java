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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.Keyspace;

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
public class ViewComplexUpdatesTest extends ViewAbstractParameterizedTest
{
    @Test
    public void testUpdateColumnNotInViewWithFlush() throws Throwable
    {
        testUpdateColumnNotInView(true);
    }

    @Test
    public void testUpdateColumnNotInViewWithoutFlush() throws Throwable
    {
        // CASSANDRA-13127
        testUpdateColumnNotInView(false);
    }

    private void testUpdateColumnNotInView(boolean flush) throws Throwable
    {
        // CASSANDRA-13127: if base column not selected in view are alive, then pk of view row should be alive
        String baseTable = createTable("create table %s (p int, c int, v1 int, v2 int, primary key(p, c))");
        Keyspace ks = Keyspace.open(keyspace());

        String mv = createView("CREATE MATERIALIZED VIEW %s AS SELECT p, c from %s " +
                               "WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p)");
        ks.getColumnFamilyStore(mv).disableAutoCompaction();

        updateView("UPDATE %s USING TIMESTAMP 0 SET v1 = 1 WHERE p = 0 AND c = 0");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, 1, null));
        assertRowsIgnoringOrder(executeView("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        updateView("DELETE v1 FROM %s USING TIMESTAMP 1 WHERE p = 0 AND c = 0");

        if (flush)
            Util.flush(ks);

        assertEmpty(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
        assertEmpty(executeView("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));

        // shadowed by tombstone
        updateView("UPDATE %s USING TIMESTAMP 1 SET v1 = 1 WHERE p = 0 AND c = 0");

        if (flush)
            Util.flush(ks);

        assertEmpty(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
        assertEmpty(executeView("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));

        updateView("UPDATE %s USING TIMESTAMP 2 SET v2 = 1 WHERE p = 0 AND c = 0");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, null, 1));
        assertRowsIgnoringOrder(executeView("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        updateView("DELETE v1 FROM %s USING TIMESTAMP 3 WHERE p = 0 AND c = 0");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, null, 1));
        assertRowsIgnoringOrder(executeView("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        updateView("DELETE v2 FROM %s USING TIMESTAMP 4 WHERE p = 0 AND c = 0");

        if (flush)
            Util.flush(ks);

        assertEmpty(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
        assertEmpty(executeView("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));

        updateView("UPDATE %s USING TTL 3 SET v2 = 1 WHERE p = 0 AND c = 0");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, null, 1));
        assertRowsIgnoringOrder(executeView("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        Thread.sleep(TimeUnit.SECONDS.toMillis(3));

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
        assertRowsIgnoringOrder(executeView("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));

        updateView("UPDATE %s SET v2 = 1 WHERE p = 0 AND c = 0");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, null, 1));
        assertRowsIgnoringOrder(executeView("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        assertInvalidMessage(String.format("Cannot drop column v2 on base table %s with materialized views", baseTable), "ALTER TABLE %s DROP v2");
        // // drop unselected base column, unselected metadata should be removed, thus view row is dead
        // updateView("ALTER TABLE %s DROP v2");
        // assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
        // assertRowsIgnoringOrder(executeView("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
        // assertRowsIgnoringOrder(execute("SELECT * from %s"));
        // assertRowsIgnoringOrder(executeView("SELECT * from %s"));
    }

    @Test
    public void testPartialUpdateWithUnselectedCollectionsWithFlush() throws Throwable
    {
        testPartialUpdateWithUnselectedCollections(true);
    }

    @Test
    public void testPartialUpdateWithUnselectedCollectionsWithoutFlush() throws Throwable
    {
        testPartialUpdateWithUnselectedCollections(false);
    }

    private void testPartialUpdateWithUnselectedCollections(boolean flush) throws Throwable
    {
        String baseTable = createTable("CREATE TABLE %s (k int, c int, a int, b int, l list<int>, s set<int>, m map<int,int>, PRIMARY KEY (k, c))");
        String mv = createView("CREATE MATERIALIZED VIEW %s AS SELECT a, b, c, k from %s " +
                               "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)");
        Keyspace ks = Keyspace.open(keyspace());
        ks.getColumnFamilyStore(mv).disableAutoCompaction();

        updateView("UPDATE %s SET l=l+[1,2,3] WHERE k = 1 AND c = 1");
        if (flush)
            Util.flush(ks);
        assertRows(executeView("SELECT * from %s"), row(1, 1, null, null));

        updateView("UPDATE %s SET l=l-[1,2] WHERE k = 1 AND c = 1");
        if (flush)
            Util.flush(ks);
        assertRows(executeView("SELECT * from %s"), row(1, 1, null, null));

        updateView("UPDATE %s SET b=3 WHERE k=1 AND c=1");
        if (flush)
            Util.flush(ks);
        assertRows(executeView("SELECT * from %s"), row(1, 1, null, 3));

        updateView("UPDATE %s SET b=null, l=l-[3], s=s-{3} WHERE k = 1 AND c = 1");
        if (flush)
        {
            Util.flush(ks);
            ks.getColumnFamilyStore(mv).forceMajorCompaction();
        }
        assertRowsIgnoringOrder(execute("SELECT k,c,a,b from %s"));
        assertRowsIgnoringOrder(executeView("SELECT * from %s"));

        updateView("UPDATE %s SET m=m+{3:3}, l=l-[1], s=s-{2} WHERE k = 1 AND c = 1");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(execute("SELECT k,c,a,b from %s"), row(1, 1, null, null));
        assertRowsIgnoringOrder(executeView("SELECT * from %s"), row(1, 1, null, null));

        assertInvalidMessage(String.format("Cannot drop column m on base table %s with materialized views", baseTable), "ALTER TABLE %s DROP m");
        // executeNet(version, "ALTER TABLE %s DROP m");
        // ks.getColumnFamilyStore(mv).forceMajorCompaction();
        // assertRowsIgnoringOrder(execute("SELECT k,c,a,b from %s WHERE k = 1 AND c = 1"));
        // assertRowsIgnoringOrder(executeView("SELECT * from %s WHERE k = 1 AND c = 1"));
        // assertRowsIgnoringOrder(execute("SELECT k,c,a,b from %s"));
        // assertRowsIgnoringOrder(executeView("SELECT * from %s"));
    }

    @Test
    public void testUpdateWithColumnTimestampSmallerThanPkWithFlush() throws Throwable
    {
        testUpdateWithColumnTimestampSmallerThanPk(true);
    }

    @Test
    public void testUpdateWithColumnTimestampSmallerThanPkWithoutFlush() throws Throwable
    {
        testUpdateWithColumnTimestampSmallerThanPk(false);
    }

    private void testUpdateWithColumnTimestampSmallerThanPk(boolean flush) throws Throwable
    {
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        Keyspace ks = Keyspace.open(keyspace());

        String mv = createView("create materialized view %s as select * from %s " +
                               "where p is not null and v1 is not null primary key (v1, p)");
        ks.getColumnFamilyStore(mv).disableAutoCompaction();

        // reset value
        updateView("Insert into %s (p, v1, v2) values (3, 1, 3) using timestamp 6;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, 3, 6L));
        // increase pk's timestamp to 20
        updateView("Insert into %s (p) values (3) using timestamp 20;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, 3, 6L));
        // change v1's to 2 and remove existing view row with ts7
        updateView("UPdate %s using timestamp 7 set v1 = 2 where p = 3;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s"), row(2, 3, 3, 6L));
        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s" + " limit 1"), row(2, 3, 3, 6L));
        // change v1's to 1 and remove existing view row with ts8
        updateView("UPdate %s using timestamp 8 set v1 = 1 where p = 3;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, 3, 6L));
    }

    @Test
    public void testUpdateWithColumnTimestampBiggerThanPkWithFlush() throws Throwable
    {
        // CASSANDRA-11500
        testUpdateWithColumnTimestampBiggerThanPk(true);
    }

    @Test
    public void testUpdateWithColumnTimestampBiggerThanPkWithoutFlush() throws Throwable
    {
        // CASSANDRA-11500
        testUpdateWithColumnTimestampBiggerThanPk(false);
    }

    private void testUpdateWithColumnTimestampBiggerThanPk(boolean flush) throws Throwable
    {
        // CASSANDRA-11500 able to shadow old view row with column ts greater tahn pk's ts and re-insert the view row
        String baseTable = createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int);");

        Keyspace ks = Keyspace.open(keyspace());

        String mv = createView("CREATE MATERIALIZED VIEW %s AS SELECT * from %s " +
                               "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)");
        ks.getColumnFamilyStore(mv).disableAutoCompaction();
        updateView("DELETE FROM %s USING TIMESTAMP 0 WHERE k = 1;");
        if (flush)
            Util.flush(ks);
        // sstable-1, Set initial values TS=1
        updateView("INSERT INTO %s(k, a, b) VALUES (1, 1, 1) USING TIMESTAMP 1;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(executeView("SELECT k,a,b from %s"), row(1, 1, 1));
        updateView("UPDATE %s USING TIMESTAMP 10 SET b = 2 WHERE k = 1;");
        assertRowsIgnoringOrder(executeView("SELECT k,a,b from %s"), row(1, 1, 2));
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(executeView("SELECT k,a,b from %s"), row(1, 1, 2));
        updateView("UPDATE %s USING TIMESTAMP 2 SET a = 2 WHERE k = 1;");
        assertRowsIgnoringOrder(executeView("SELECT k,a,b from %s"), row(1, 2, 2));
        if (flush)
            Util.flush(ks);
        ks.getColumnFamilyStore(mv).forceMajorCompaction();
        assertRowsIgnoringOrder(executeView("SELECT k,a,b from %s"), row(1, 2, 2));
        assertRowsIgnoringOrder(executeView("SELECT k,a,b from %s limit 1"), row(1, 2, 2));
        updateView("UPDATE %s USING TIMESTAMP 11 SET a = 1 WHERE k = 1;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(executeView("SELECT k,a,b from %s"), row(1, 1, 2));
        assertRowsIgnoringOrder(execute("SELECT k,a,b from %s"), row(1, 1, 2));

        // set non-key base column as tombstone, view row is removed with shadowable
        updateView("UPDATE %s USING TIMESTAMP 12 SET a = null WHERE k = 1;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(executeView("SELECT k,a,b from %s"));
        assertRowsIgnoringOrder(execute("SELECT k,a,b from %s"), row(1, null, 2));

        // column b should be alive
        updateView("UPDATE %s USING TIMESTAMP 13 SET a = 1 WHERE k = 1;");
        if (flush)
            Util.flush(ks);
        assertRowsIgnoringOrder(executeView("SELECT k,a,b from %s"), row(1, 1, 2));
        assertRowsIgnoringOrder(execute("SELECT k,a,b from %s"), row(1, 1, 2));

        assertInvalidMessage(String.format("Cannot drop column a on base table %s with materialized views", baseTable), "ALTER TABLE %s DROP a");
    }
}
