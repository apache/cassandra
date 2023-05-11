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

import java.math.BigDecimal;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

public class DecimalLargeValueTest extends SAITester
{
    @Before
    public void createTableAndIndex()
    {
        requireNetwork();

        createTable("CREATE TABLE %s (pk int, ck int, dec decimal, PRIMARY KEY (pk, ck))");

        createIndex("CREATE CUSTOM INDEX ON %s(dec) USING 'StorageAttachedIndex'");

        disableCompaction();
    }

    /**
     * This test tries to induce rounding errors involving decimal values with wide significands.
     *
     * Two values are indexed:
     * <ul>
     * <li>1.0</li>
     * <li>1.(510 zeros)1</li>
     * </ul>
     */
    @Test
    public void runQueriesWithDecimalValueCollision() throws Throwable
    {
        final int significandSizeInDecimalDigits = 512;
        // String.repeat(int) exists in JDK 11 and later, but this line was introduced on JDK 8
        String wideDecimalString = "1." + StringUtils.repeat('0', significandSizeInDecimalDigits - 2) + '1';
        BigDecimal wideDecimal = new BigDecimal(wideDecimalString);
        // Sanity checks that this value was actually constructed as intended
        Preconditions.checkState(wideDecimal.precision() == significandSizeInDecimalDigits,
                                 "expected precision %s, but got %s; string representation is \"%s\"",
                                 significandSizeInDecimalDigits, wideDecimal.precision(), wideDecimalString);
        Preconditions.checkState(wideDecimalString.equals(wideDecimal.toPlainString()),
                                 "expected: %s; actual: %s", wideDecimalString, wideDecimal.toPlainString());

        execute("INSERT INTO %s (pk, ck, dec) VALUES (0, 1, 1.0)");
        execute("INSERT INTO %s (pk, ck, dec) VALUES (2, 0, " + wideDecimalString + ')');

        // EQ queries
        assertRows(execute("SELECT * FROM %s WHERE dec = 1.0"),
                row(0, 1, BigDecimal.valueOf(1.0D)));

        assertRows(execute("SELECT * FROM %s WHERE dec = " + wideDecimalString),
                row(2, 0, wideDecimal));

        // LT/LTE queries
        assertRows(execute("SELECT * FROM %s WHERE dec < " + wideDecimalString),
                row(0, 1, BigDecimal.valueOf(1.0D)));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE dec <= " + wideDecimalString),
                row(0, 1, BigDecimal.valueOf(1.0D)),
                row(2, 0, wideDecimal));

        assertEmpty(execute("SELECT * FROM %s WHERE dec < 1.0"));

        assertRows(execute("SELECT * FROM %s WHERE dec <= 1.0"),
                row(0, 1, BigDecimal.valueOf(1.0D)));

        // GT/GTE queries
        assertRows(execute("SELECT * FROM %s WHERE dec > 1.0"),
                row(2, 0, wideDecimal));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE dec >= " + wideDecimalString),
                row(2, 0, wideDecimal));

        assertEmpty(execute("SELECT * FROM %s WHERE dec > " + wideDecimalString));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE dec >= 1.0"),
                row(0, 1, BigDecimal.valueOf(1.0D)),
                row(2, 0, wideDecimal));
    }
    /**
     * This is a control method with small (two-significant-digit) values.
     */
    @Test
    public void runQueriesWithoutCollisions() throws Throwable
    {
        execute("INSERT INTO %s (pk, ck, dec) VALUES (-2, 1, 2.2)");
        execute("INSERT INTO %s (pk, ck, dec) VALUES (-2, 2, 2.2)");
        execute("INSERT INTO %s (pk, ck, dec) VALUES (-1, 1, 1.1)");
        execute("INSERT INTO %s (pk, ck, dec) VALUES (0, 1, 0)");
        execute("INSERT INTO %s (pk, ck, dec) VALUES (1, 1, 1.1)");
        execute("INSERT INTO %s (pk, ck, dec) VALUES (2, 1, 2.2)");
        execute("INSERT INTO %s (pk, ck, dec) VALUES (2, 2, 2.2)");

        // EQ queries
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE dec = 1.1"),
                row(-1, 1, BigDecimal.valueOf(1.1D)),
                row(1, 1, BigDecimal.valueOf(1.1D)));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE dec = 2.2"),
                row(-2, 1, BigDecimal.valueOf(2.2D)),
                row(-2, 2, BigDecimal.valueOf(2.2D)),
                row(2, 1, BigDecimal.valueOf(2.2D)),
                row(2, 2, BigDecimal.valueOf(2.2D)));

        // LT/LTE queries
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE dec < 1.1"),
                row(0, 1, BigDecimal.valueOf(0)));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE dec <= 1.1"),
                row(-1, 1, BigDecimal.valueOf(1.1D)),
                row(0, 1, BigDecimal.valueOf(0)),
                row(1, 1, BigDecimal.valueOf(1.1D)));

        // GT/GTE queries
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE dec > 1.1"),
                row(-2, 1, BigDecimal.valueOf(2.2D)),
                row(-2, 2, BigDecimal.valueOf(2.2D)),
                row(2, 1, BigDecimal.valueOf(2.2D)),
                row(2, 2, BigDecimal.valueOf(2.2D)));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE dec >= 1.1"),
                row(-2, 1, BigDecimal.valueOf(2.2D)),
                row(-2, 2, BigDecimal.valueOf(2.2D)),
                row(-1, 1, BigDecimal.valueOf(1.1D)),
                row(1, 1, BigDecimal.valueOf(1.1D)),
                row(2, 1, BigDecimal.valueOf(2.2D)),
                row(2, 2, BigDecimal.valueOf(2.2D)));
    }
}
