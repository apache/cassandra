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

package org.apache.cassandra.cql3.functions.masking;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.marshal.AbstractType;

import static java.lang.String.format;

/**
 * {@link ColumnMaskTester} verifying that masks can be applied to columns in any position (partition key columns,
 * clustering key columns, static columns and regular columns). The columns of any depending materialized views should
 * be udpated accordingly.
 */
@RunWith(Parameterized.class)
public abstract class ColumnMaskInAnyPositionTester extends ColumnMaskTester
{
    /** The column mask as expressed in CQL statements right after the {@code MASKED WITH} keywords. */
    @Parameterized.Parameter
    public String mask;

    /** The type of the masked column */
    @Parameterized.Parameter(1)
    public String type;

    /** The types of the tested masking function partial arguments. */
    @Parameterized.Parameter(2)
    public List<AbstractType<?>> argumentTypes;

    /** The serialized values of the tested masking function partial arguments. */
    @Parameterized.Parameter(3)
    public List<ByteBuffer> argumentValues;

    @Test
    public void testCreateTableWithMaskedColumns() throws Throwable
    {
        // Nothing is masked
        createTable("CREATE TABLE %s (k int, c int, r int, s int static, PRIMARY KEY(k, c))");
        assertTableColumnsAreNotMasked("k", "c", "r", "s");

        // Masked partition key
        createTable(format("CREATE TABLE %%s (k %s MASKED WITH %s PRIMARY KEY, r int)", type, mask));
        assertTableColumnsAreMasked("k");
        assertTableColumnsAreNotMasked("r");

        // Masked partition key component
        createTable(format("CREATE TABLE %%s (k1 int, k2 %s MASKED WITH %s, r int, PRIMARY KEY(k1, k2))", type, mask));
        assertTableColumnsAreMasked("k2");
        assertTableColumnsAreNotMasked("k1", "r");

        // Masked clustering key
        createTable(format("CREATE TABLE %%s (k int, c %s MASKED WITH %s, r int, PRIMARY KEY (k, c))", type, mask));
        assertTableColumnsAreMasked("c");
        assertTableColumnsAreNotMasked("k", "r");

        // Masked clustering key with reverse order
        createTable(format("CREATE TABLE %%s (k int, c %s MASKED WITH %s, r int, PRIMARY KEY (k, c)) " +
                           "WITH CLUSTERING ORDER BY (c DESC)", type, mask));
        assertTableColumnsAreMasked("c");
        assertTableColumnsAreNotMasked("k", "r");

        // Masked clustering key component
        createTable(format("CREATE TABLE %%s (k int, c1 int, c2 %s MASKED WITH %s, r int, PRIMARY KEY (k, c1, c2))", type, mask));
        assertTableColumnsAreMasked("c2");
        assertTableColumnsAreNotMasked("k", "c1", "r");

        // Masked regular column
        createTable(format("CREATE TABLE %%s (k int PRIMARY KEY, r1 %s MASKED WITH %s, r2 int)", type, mask));
        assertTableColumnsAreMasked("r1");
        assertTableColumnsAreNotMasked("k", "r2");

        // Masked static column
        createTable(format("CREATE TABLE %%s (k int, c int, r int, s %s STATIC MASKED WITH %s, PRIMARY KEY (k, c))", type, mask));
        assertTableColumnsAreMasked("s");
        assertTableColumnsAreNotMasked("k", "c", "r");

        // Multiple masked columns
        createTable(format("CREATE TABLE %%s (" +
                           "k1 int, k2 %s MASKED WITH %s, " +
                           "c1 int, c2 %s MASKED WITH %s, " +
                           "r1 int, r2 %s MASKED WITH %s, " +
                           "s1 int static, s2 %s static MASKED WITH %s, " +
                           "PRIMARY KEY((k1, k2), c1, c2))",
                           type, mask, type, mask, type, mask, type, mask));
        assertTableColumnsAreMasked("k2", "c2", "r2", "s2");
        assertTableColumnsAreNotMasked("k1", "c1", "r1", "s1");
    }

    @Test
    public void testCreateTableWithMaskedColumnsAndMaterializedView() throws Throwable
    {
        createTable(format("CREATE TABLE %%s (" +
                           "k1 int, k2 %s MASKED WITH %s, " +
                           "c1 int, c2 %s MASKED WITH %s, " +
                           "r1 int, r2 %s MASKED WITH %s, " +
                           "s1 int static, s2 %s static MASKED WITH %s, " +
                           "PRIMARY KEY((k1, k2), c1, c2))",
                           type, mask, type, mask, type, mask, type, mask));
        createView("CREATE MATERIALIZED VIEW %s AS SELECT k1, k2, c1, c2, r1, r2 FROM %s " +
                   "WHERE k1 IS NOT NULL AND k2 IS NOT NULL " +
                   "AND c1 IS NOT NULL AND c2 IS NOT NULL " +
                   "AND r1 IS NOT NULL AND r2 IS NOT NULL " +
                   "PRIMARY KEY (r2, c2, c1, k2, k1)");

        assertTableColumnsAreMasked("k2", "c2", "r2", "s2");
        assertTableColumnsAreNotMasked("k1", "c1", "r1", "s1");

        assertViewColumnsAreMasked("k2", "c2", "r2");
        assertViewColumnsAreNotMasked("k1", "c1", "r1");
    }

    @Test
    public void testAlterTableWithMaskedColumns() throws Throwable
    {
        // Create the table to be altered
        createTable(format("CREATE TABLE %%s (k %s, c %<s, r1 %<s, r2 %<s MASKED WITH %s, r3 %s, s %<s static, " +
                           "PRIMARY KEY (k, c))", type, mask, type));
        assertTableColumnsAreMasked("r2");
        assertTableColumnsAreNotMasked("k", "c", "r1", "r3", "s");

        // Add new masked column
        alterTable(format("ALTER TABLE %%s ADD r4 %s MASKED WITH %s", type, mask));
        assertTableColumnsAreMasked("r2", "r4");
        assertTableColumnsAreNotMasked("k", "c", "r1", "r3", "s");

        // Set mask for an existing but unmasked column
        alterTable(format("ALTER TABLE %%s ALTER r1 MASKED WITH %s", mask));
        assertTableColumnsAreMasked("r1", "r2", "r4");

        // Unmask a masked column
        alterTable("ALTER TABLE %s ALTER r1 DROP MASKED");
        alterTable("ALTER TABLE %s ALTER r2 DROP MASKED");
        assertTableColumnsAreMasked("r4");
        assertTableColumnsAreNotMasked("r1", "r2", "r3");

        // Mask and disable mask for primary key
        alterTable(format("ALTER TABLE %%s ALTER k MASKED WITH %s", mask));
        assertTableColumnsAreMasked("k");
        alterTable("ALTER TABLE %s ALTER k DROP MASKED");
        assertTableColumnsAreNotMasked("k");

        // Mask and disable mask for clustering key
        alterTable(format("ALTER TABLE %%s ALTER c MASKED WITH %s", mask));
        assertTableColumnsAreMasked("c");
        alterTable("ALTER TABLE %s ALTER c DROP MASKED");
        assertTableColumnsAreNotMasked("c");

        // Mask and disable mask for static column
        alterTable(format("ALTER TABLE %%s ALTER s MASKED WITH %s", mask));
        assertTableColumnsAreMasked("s");
        alterTable("ALTER TABLE %s ALTER s DROP MASKED");
        assertTableColumnsAreNotMasked("s");

        // Add multiple masked columns within the same query
        alterTable(format("ALTER TABLE %%s ADD (" +
                          "r5 %s MASKED WITH %s, " +
                          "r6 %s, " +
                          "r7 %s MASKED WITH %s, " +
                          "r8 %s)",
                          type, mask, type, type, mask, type));
        assertTableColumnsAreMasked("r5", "r7");
        assertTableColumnsAreNotMasked("r6", "r8");
    }

    @Test
    public void testAlterTableWithMaskedColumnsAndMaterializedView() throws Throwable
    {
        createTable(format("CREATE TABLE %%s (" +
                           "k %s, c %<s, r1 %<s, r2 %<s, s1 %<s static, s2 %<s static, " +
                           "PRIMARY KEY(k, c))", type));
        createView("CREATE MATERIALIZED VIEW %s AS SELECT k, c, r2 FROM %s " +
                   "WHERE k IS NOT NULL AND c IS NOT NULL " +
                   "AND r1 IS NOT NULL AND r2 IS NOT NULL " +
                   "PRIMARY KEY (r2, c, k)");

        // Adding a column to the table doesn't have an effect on the view
        alterTable(format("ALTER TABLE %%s ADD r3 %s MASKED WITH %s", type, mask));
        assertTableColumnsAreMasked("r3");
        assertTableColumnsAreNotMasked("k", "c", "r1", "r2", "s1", "s2");
        assertViewColumnsAreNotMasked("k", "c", "r2");

        // Masking a column that is not part of the view doesn't have an effect on the view
        alterTable(format("ALTER TABLE %%s ALTER r1 MASKED WITH %s", mask));
        alterTable(format("ALTER TABLE %%s ALTER s1 MASKED WITH %s", mask));
        assertTableColumnsAreMasked("r1", "r3", "s1");
        assertTableColumnsAreNotMasked("k", "c", "r2", "s2");
        assertViewColumnsAreNotMasked("k", "c", "r2");

        // Masking a column that is part of the view should have an effect on the view
        alterTable(format("ALTER TABLE %%s ALTER r2 MASKED WITH %s", mask));
        assertTableColumnsAreMasked("r1", "r2", "r3", "s1");
        assertTableColumnsAreNotMasked("k", "c", "s2");
        assertViewColumnsAreMasked("r2");
        assertViewColumnsAreNotMasked("k", "c");

        // Mask the rest of the columns
        alterTable(format("ALTER TABLE %%s ALTER k MASKED WITH %s", mask));
        alterTable(format("ALTER TABLE %%s ALTER c MASKED WITH %s", mask));
        alterTable(format("ALTER TABLE %%s ALTER s2 MASKED WITH %s", mask));
        assertTableColumnsAreMasked("k", "c", "r1", "r2", "r3", "s1", "s2");
        assertViewColumnsAreMasked("k", "c", "r2");

        // Unmask a column that is part of the view
        alterTable("ALTER TABLE %s ALTER r2 DROP MASKED");
        assertTableColumnsAreMasked("k", "c", "r1", "r3", "s1", "s2");
        assertTableColumnsAreNotMasked("r2");
        assertViewColumnsAreMasked("k", "c");
        assertViewColumnsAreNotMasked("r2");

        // Unmask the rest of the columns
        alterTable("ALTER TABLE %s ALTER k DROP MASKED");
        alterTable("ALTER TABLE %s ALTER c DROP MASKED");
        alterTable("ALTER TABLE %s ALTER r1 DROP MASKED");
        alterTable("ALTER TABLE %s ALTER r3 DROP MASKED");
        alterTable("ALTER TABLE %s ALTER s1 DROP MASKED");
        alterTable("ALTER TABLE %s ALTER s2 DROP MASKED");
        assertTableColumnsAreNotMasked("k", "c", "r1", "r2", "r3", "s1", "s2");
        assertViewColumnsAreNotMasked("k", "c", "r2");
    }

    private String functionName()
    {
        if (mask.equals("DEFAULT"))
            return "mask_default";

        return StringUtils.remove(StringUtils.substringBefore(mask, "("), KEYSPACE + ".");
    }

    private void assertTableColumnsAreMasked(String... columns) throws Throwable
    {
        for (String column : columns)
        {
            assertColumnIsMasked(currentTable(), column, functionName(), argumentTypes, argumentValues);
        }
    }

    private void assertViewColumnsAreMasked(String... columns) throws Throwable
    {
        for (String column : columns)
        {
            assertColumnIsMasked(currentView(), column, functionName(), argumentTypes, argumentValues);
        }
    }
}
