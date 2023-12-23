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

package org.apache.cassandra.harry.corruptor;

import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.harry.data.ResultSetRow;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.operations.DeleteHelper;
import org.apache.cassandra.harry.util.BitSet;

// removes/hides the value of one of the columns that was previously set
public class HideValueCorruptor implements RowCorruptor
{
    private final SchemaSpec schema;
    private final OpSelectors.Clock clock;
    private final EntropySource rng;

    public HideValueCorruptor(SchemaSpec schemaSpec,
                              OpSelectors.Clock clock)
    {
        this.schema = schemaSpec;
        this.clock = clock;
        this.rng = new JdkRandomEntropySource(1L);
    }

    // Can corrupt any row that has at least one written non-null value
    public boolean canCorrupt(ResultSetRow row)
    {
        for (int idx = 0; idx < row.lts.length; idx++)
        {
            if (row.lts[idx] != Model.NO_TIMESTAMP)
                return true;
        }
        return false;
    }

    public CompiledStatement corrupt(ResultSetRow row)
    {
        BitSet mask;
        // Corrupt a static row, if it is available and if RNG says so
        if (row.hasStaticColumns() && rng.nextBoolean())
        {
            int cnt = 0;
            int idx;
            do
            {
                idx = rng.nextInt(row.slts.length);
                cnt++;
            }
            while (row.slts[idx] == Model.NO_TIMESTAMP && cnt < 10);

            if (row.slts[idx] != Model.NO_TIMESTAMP)
            {
                mask = BitSet.allUnset(schema.allColumns.size());
                mask.set(schema.staticColumnsOffset + idx);

                return DeleteHelper.deleteColumn(schema,
                                                 row.pd,
                                                 mask,
                                                 schema.regularAndStaticColumnsMask(),
                                                 clock.rts(clock.peek()));
            }
        }

        Set<Integer> tried = new HashSet<>();
        int idx;
        do
        {
            if (tried.size() == row.lts.length)
                throw new IllegalStateException(String.format("Could not corrupt after trying all %s indexes", tried));
            idx = rng.nextInt(row.lts.length);
            tried.add(idx);
        }
        while (row.lts[idx] == Model.NO_TIMESTAMP);

        mask = BitSet.allUnset(schema.allColumns.size());
        mask.set(schema.regularColumnsOffset + idx);

        return DeleteHelper.deleteColumn(schema,
                                         row.pd,
                                         row.cd,
                                         mask,
                                         schema.regularAndStaticColumnsMask(),
                                         clock.rts(clock.peek()));
    }
}
