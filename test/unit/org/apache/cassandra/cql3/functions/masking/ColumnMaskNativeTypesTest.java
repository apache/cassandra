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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQL3Type;

import static java.lang.String.format;
import static java.util.Collections.emptyList;

/**
 * {@link ColumnMaskTester} verifying that we can attach column masks to table columns with any native data type.
 */
@RunWith(Parameterized.class)
public class ColumnMaskNativeTypesTest extends ColumnMaskTester
{
    /** The type of the column. */
    @Parameterized.Parameter
    public CQL3Type.Native type;

    @Parameterized.Parameters(name = "type={0}")
    public static Collection<Object[]> options()
    {
        List<Object[]> parameters = new ArrayList<>();
        for (CQL3Type.Native type : CQL3Type.Native.values())
        {
            if (type != CQL3Type.Native.EMPTY)
                parameters.add(new Object[]{ type });
        }
        return parameters;
    }

    @Test
    public void testNativeDataTypes() throws Throwable
    {
        String def = format("%s MASKED WITH DEFAULT", type);
        String keyDef = type == CQL3Type.Native.COUNTER || type == CQL3Type.Native.DURATION
                        ? "int MASKED WITH DEFAULT" : def;
        String staticDef = format("%s STATIC MASKED WITH DEFAULT", type);

        // Create table with masks
        String table = createTable(format("CREATE TABLE %%s (k %s, c %<s, r %s, s %s, PRIMARY KEY(k, c))", keyDef, def, staticDef));
        assertColumnIsMasked(table, "k", "mask_default", emptyList(), emptyList());
        assertColumnIsMasked(table, "c", "mask_default", emptyList(), emptyList());
        assertColumnIsMasked(table, "r", "mask_default", emptyList(), emptyList());
        assertColumnIsMasked(table, "s", "mask_default", emptyList(), emptyList());

        // Alter column masks
        alterTable("ALTER TABLE %s ALTER k MASKED WITH mask_null()");
        alterTable("ALTER TABLE %s ALTER c MASKED WITH mask_null()");
        alterTable("ALTER TABLE %s ALTER r MASKED WITH mask_null()");
        alterTable("ALTER TABLE %s ALTER s MASKED WITH mask_null()");
        assertColumnIsMasked(table, "k", "mask_null", emptyList(), emptyList());
        assertColumnIsMasked(table, "c", "mask_null", emptyList(), emptyList());
        assertColumnIsMasked(table, "r", "mask_null", emptyList(), emptyList());
        assertColumnIsMasked(table, "s", "mask_null", emptyList(), emptyList());

        // Drop masks
        alterTable("ALTER TABLE %s ALTER k DROP MASKED");
        alterTable("ALTER TABLE %s ALTER c DROP MASKED");
        alterTable("ALTER TABLE %s ALTER r DROP MASKED");
        alterTable("ALTER TABLE %s ALTER s DROP MASKED");
        assertTableColumnsAreNotMasked("k", "c", "r", "s");
    }
}

