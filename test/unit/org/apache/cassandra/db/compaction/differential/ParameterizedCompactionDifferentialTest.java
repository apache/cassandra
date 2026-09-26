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

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Runs the path-agnostic {@link DifferentialSchemas#minimalCorpus() minimal schema corpus} through the
 * cursor-vs-iterator compaction differential across BOTH sstable formats (BIG and BTI). Each
 * (format, shape) cell is a real cursor-vs-iterator comparison: the harness asserts the cursor path
 * actually ran and that the output is in the selected format, then asserts byte + logical equivalence
 * against the iterator path.
 *
 * <p>The burn counterpart is {@link BurnCompactionDifferentialTest}.
 */
@RunWith(Parameterized.class)
public class ParameterizedCompactionDifferentialTest extends DifferentialCompactionTester
{
    /** Fast-matrix scale: base shape counts, unmultiplied. */
    static final int MATRIX_SCALE = 1;

    @Parameterized.Parameter(0)
    public String format;

    @Parameterized.Parameter(1)
    public DifferentialSchema schema;

    @Parameterized.Parameters(name = "{0}-{1}")
    public static List<Object[]> parameters()
    {
        List<Object[]> params = new ArrayList<>();
        for (String format : new String[]{ "big", "bti" })
            for (DifferentialSchema schema : DifferentialSchemas.minimalCorpus())
                params.add(new Object[]{ format, schema });
        return params;
    }

    @Before
    public void selectFormat()
    {
        selectSSTableFormat(format);
    }

    @After
    public void restoreFormat()
    {
        restoreSelectedFormat();
    }

    @Test
    public void cursorMatchesIteratorForShape() throws Exception
    {
        ColumnFamilyStore cfs = writeAndGuardShape(schema, MATRIX_SCALE);
        assertCursorMatchesIteratorForShape(cfs, schema);
    }
}
