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

package org.apache.cassandra.db.compaction.differential;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_DIFFERENTIAL_WIDE_REGULARS;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_DIFFERENTIAL_WIDE_STATICS;

/**
 * Compacts a table that has about 2000 columns: 1800 regular and 200 static.
 *
 * The columns use 20 different types: primitives, blobs, uuids, inet, duration, frozen
 * collections, tuples, vectors, and multi-cell maps, sets, lists and UDTs. One column in five is
 * multi-cell. The 70-column tests reach the same limits. This test goes far past them.
 * It covers:
 *
 * <ul>
 *   <li>the wire format of a large column subset, with thousands of indexes, in both encoding
 *       modes, and at the exact boundary where the mode changes. That boundary is at
 *       present == supersetCount / 2;</li>
 *   <li>hundreds of complex-column markers in one row, which makes the marker arrays grow;</li>
 *   <li>rows of many shapes: full rows with HAS_ALL_COLUMNS, sparse rows, rows whose present
 *       columns are only at the end, rows of one column, rows made by UPDATE alone, and rows
 *       that hold liveness alone;</li>
 *   <li>hundreds of cell tombstones, tombstones from an overwrite with null, complex deletions
 *       on multi-cell columns, element updates and element tombstones;</li>
 *   <li>a TTL on single cells, both live and expired;</li>
 *   <li>wide static blocks, which carry their own subset encoding;</li>
 *   <li>range deletes, row deletes and partition deletes above all of the above.</li>
 * </ul>
 *
 * This scenario has its own class rather than a case in EdgeCase. Its DDL and its prepared
 * statements are large, and the setup takes most of the run time. A subclass runs the same
 * scenario on the BTI format.
 *
 * You can change the width with two system properties. The defaults give 1800 regular columns
 * and 200 static columns. The properties must reach the forked test JVM through -Dtest.jvm.args:
 *
 * <pre>
 *   ant testsome -Dtest.name=...PathologicalWideTableDifferentialCompactionTest \
 *       -Dtest.jvm.args="-Dcassandra.test.differential.wide.regulars=5000
 *                        -Dcassandra.test.differential.wide.statics=500"
 * </pre>
 *
 * Keep the regular count at 128 or above. A smaller count drops the coverage of the subset
 * encodings for more than 64 columns, and of the mode boundary. The width sets everything else:
 * the boundary rows, the sparse rows, and the sets of deleted columns.
 */
public class PathologicalWideTableDifferentialCompactionTest extends DifferentialCompactionTester
{
    private static final int REGULARS = TEST_DIFFERENTIAL_WIDE_REGULARS.getInt();
    private static final int STATICS = TEST_DIFFERENTIAL_WIDE_STATICS.getInt();
    private static final int PALETTE = 20;

    private String udt;

    private String typeFor(int i)
    {
        switch (i % PALETTE)
        {
            case 0: return "bigint";
            case 1: return "text";
            case 2: return "int";
            case 3: return "double";
            case 4: return "blob";
            case 5: return "uuid";
            case 6: return "boolean";
            case 7: return "decimal";
            case 8: return "varint";
            case 9: return "inet";
            case 10: return "duration";
            case 11: return "frozen<list<int>>";
            case 12: return "frozen<map<text, int>>";
            case 13: return "tuple<int, text>";
            case 14: return "vector<float, 3>";
            case 15: return "map<text, bigint>";   // multi-cell
            case 16: return "set<int>";            // multi-cell
            case 17: return "list<text>";          // multi-cell
            case 18: return udt;                   // multi-cell
            case 19: return "timestamp";
            default: throw new AssertionError();
        }
    }

    private Object valueFor(int i, int salt) throws Exception
    {
        long v = i * 31L + salt;
        switch (i % PALETTE)
        {
            case 0: return v;
            case 1: return "t" + v;
            case 2: return (int) v;
            case 3: return v / 7.0;
            case 4: return ByteBuffer.wrap(new byte[]{ (byte) v, (byte) (v >> 8), (byte) salt });
            case 5: return new UUID(v, ~v);
            case 6: return (v & 1) == 0;
            case 7: return BigDecimal.valueOf(v, 3);
            case 8: return BigInteger.valueOf(v).pow(3);
            case 9: return InetAddress.getByAddress(new byte[]{ 10, (byte) salt, (byte) (i >> 8), (byte) i });
            case 10: return Duration.newInstance(0, (int) (v % 28) + 1, (v % 1000) * 1_000_000L);
            case 11: return list((int) v, (int) v + 1);
            case 12: return map("m" + (v % 5), (int) v);
            case 13: return tuple((int) v, "tu" + v);
            case 14: return vector((float) (v % 100), salt + 0.5f, 3.25f);
            case 15: return map("k" + (v % 3), v, "shared", (long) salt);
            case 16: return set((int) (v % 50), 7);
            case 17: return list("l" + (v % 4), "x" + salt);
            case 18: return userType("a", (int) v, "b", "u" + (v % 6));
            case 19: return new Date(1_700_000_000_000L + v);
            default: throw new AssertionError();
        }
    }

    private static boolean isMultiCell(int i)
    {
        int m = i % PALETTE;
        return m >= 15 && m <= 18;
    }

    /** Inserts the primary key and the columns [start, start+count) mod REGULARS. */
    private void insertWindow(long pk, long ck, int start, int count, int salt, String using) throws Throwable
    {
        StringBuilder stmt = new StringBuilder("INSERT INTO %s (pk, ck");
        Object[] params = new Object[count + 2];
        params[0] = pk;
        params[1] = ck;
        for (int i = 0; i < count; i++)
        {
            int col = (start + i) % REGULARS;
            stmt.append(", r").append(col);
            params[i + 2] = valueFor(col, salt);
        }
        stmt.append(") VALUES (?, ?");
        stmt.append(", ?".repeat(count));
        stmt.append(')').append(using);
        execute(stmt.toString(), params);
    }

    @Test
    public void thousandsOfColumns() throws Throwable
    {
        logger.info("pathological-wide parameters: regulars={} statics={}", REGULARS, STATICS);
        udt = createType("CREATE TYPE %s (a int, b text)");

        StringBuilder ddl = new StringBuilder("CREATE TABLE %s (pk bigint, ck bigint");
        for (int i = 0; i < REGULARS; i++)
            ddl.append(", r").append(i).append(' ').append(typeFor(i));
        for (int i = 0; i < STATICS; i++)
            ddl.append(", s").append(i).append(' ').append(typeFor(i)).append(" static");
        ddl.append(", PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        createTable(ddl.toString());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 2; round++)
        {
            int salt = round * 1000;

            // pk 0 holds full rows. Each sets HAS_ALL_COLUMNS and holds about 360 complex markers.
            for (long ck = 0; ck < 2; ck++)
                insertWindow(0, ck, 0, REGULARS, salt + (int) ck, "");

            // pk 1 holds sparse rows. The offsets wrap, so some rows hold their present columns
            // only at the end.
            for (long ck = 0; ck < 10; ck++)
                insertWindow(1, ck, (int) ck * 173 + round * 61, 60, salt, "");

            // pk 2 sits at the boundary where the encoding mode changes, on both sides of it.
            // The boundary is at present == supersetCount / 2.
            insertWindow(2, 0, 0, REGULARS / 2 - 1, salt, "");
            insertWindow(2, 1, 0, REGULARS / 2, salt, "");
            insertWindow(2, 2, 0, REGULARS / 2 + 1, salt, "");

            // pk 3 holds a row of one column, a row made by UPDATE that carries no liveness, and
            // a row that holds liveness alone.
            insertWindow(3, 0, 37, 1, salt, "");
            execute("UPDATE %s SET r0 = ?, r1 = ? WHERE pk = ? AND ck = ?",
                    valueFor(0, salt), valueFor(1, salt), 3L, 1L);
            execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", 3L, 2L);

            // pk 4 holds a full row with a TTL. Round 0 also writes cells with a TTL of one
            // second, which expire before the test compacts them.
            insertWindow(4, 0, 0, REGULARS, salt, " USING TTL 86400");
            if (round == 0)
                insertWindow(4, 1, 100, 40, salt, " USING TTL 1");

            // pk 6 holds the wide static block and regular rows. The static block has its own
            // subset encoding.
            {
                StringBuilder stmt = new StringBuilder("UPDATE %s SET ");
                Object[] params = new Object[STATICS + 1];
                for (int i = 0; i < STATICS; i++)
                {
                    if (i > 0) stmt.append(", ");
                    stmt.append('s').append(i).append(" = ?");
                    params[i] = valueFor(i, salt + 7);
                }
                stmt.append(" WHERE pk = ?");
                params[STATICS] = 6L;
                execute(stmt.toString(), params);
            }
            for (long ck = 0; ck < 3; ck++)
                insertWindow(6, ck, (int) ck * 200, 30, salt, "");

            // Round 0 writes pk 7. The tombstone layer below deletes it.
            if (round == 0)
                insertWindow(7, 0, 0, 25, salt, "");

            flush();
        }

        // The tombstone layer is a third sstable. It deletes parts of everything above.
        {
            // Hundreds of cell tombstones on named columns of a full row.
            StringBuilder del = new StringBuilder("DELETE ");
            int n = 0;
            for (int i = 0; i < REGULARS && n < 300; i += 6, n++)
            {
                if (n > 0) del.append(", ");
                del.append('r').append(i);
            }
            del.append(" FROM %s WHERE pk = ? AND ck = ?");
            execute(del.toString(), 0L, 0L);

            // Complex deletions on multi-cell columns of the other full row.
            StringBuilder cdel = new StringBuilder("DELETE ");
            n = 0;
            for (int i = 15; i < REGULARS && n < 40; i += PALETTE, n++)
            {
                if (n > 0) cdel.append(", ");
                cdel.append('r').append(i);
            }
            cdel.append(" FROM %s WHERE pk = ? AND ck = ?");
            execute(cdel.toString(), 0L, 1L);

            // Element updates and element tombstones on multi-cell columns.
            execute("UPDATE %s SET r15[?] = ?, r16 = r16 + ? WHERE pk = ? AND ck = ?",
                    "fresh", 1234L, set(99), 0L, 1L);
            execute("DELETE r35[?] FROM %s WHERE pk = ? AND ck = ?", "k0", 0L, 1L);

            // Tombstones made by an overwrite with null, on a sparse row.
            StringBuilder nul = new StringBuilder("INSERT INTO %s (pk, ck");
            for (int i = 0; i < 60; i++)
                nul.append(", r").append((173 + i) % REGULARS); // the same columns as pk 1, ck 1
            nul.append(") VALUES (?, ?").append(", null".repeat(60)).append(')');
            execute(nul.toString(), 1L, 1L);

            // Deletes of static cells and of static complex columns.
            execute("DELETE s0, s1, s15, s16 FROM %s WHERE pk = ?", 6L);

            // Range deletes, row deletes and partition deletes.
            execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", 1L, 7L); // a range with no end
            execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 2L, 2L);  // a row at the boundary
            execute("DELETE FROM %s WHERE pk = ?", 7L);                 // a whole partition
            flush();
        }

        Thread.sleep(2000); // Wait for the cells with a TTL of one second to expire.

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }
}
