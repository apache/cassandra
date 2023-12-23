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

import java.util.Arrays;

import org.apache.cassandra.harry.data.ResultSetRow;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.DataGenerators;
import org.apache.cassandra.harry.gen.rng.PcgRSUFast;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.operations.WriteHelper;

/**
 * Corrupts a single value written value in the row by writing a valid, invertible value with an incorrect
 * descriptor, if row has any values written.
 */
public class ChangeValueCorruptor implements RowCorruptor
{
    private final SchemaSpec schema;
    private final OpSelectors.Clock clock;
    private final EntropySource rng;

    public ChangeValueCorruptor(SchemaSpec schemaSpec,
                                OpSelectors.Clock clock)
    {
        this.schema = schemaSpec;
        this.clock = clock;
        this.rng = new PcgRSUFast(1, 1);
    }

    // Can corrupt any row that has at least one written non-null value
    public boolean canCorrupt(ResultSetRow row)
    {
        for (int idx = 0; idx < row.lts.length; idx++)
        {
            // TODO: in addition to this, we should check if the value equals to the largest possible
            // value, since otherwise it won't sort correctly.
            if (row.lts[idx] != Model.NO_TIMESTAMP)
                return true;
        }
        return false;
    }

    public CompiledStatement corrupt(ResultSetRow row)
    {
        long[] corruptedVds = new long[row.vds.length];
        Arrays.fill(corruptedVds, DataGenerators.UNSET_DESCR);

        int idx;
        do
        {
            idx = rng.nextInt(corruptedVds.length - 1);
        } while (row.lts[idx] == Model.NO_TIMESTAMP);

        final long oldV = row.vds[idx];
        do
        {
            corruptedVds[idx] = schema.regularColumns.get(idx).type.generator().adjustEntropyDomain(rng.next());
        }
        // we need to find a value that sorts strictly greater than the current one
        while (schema.regularColumns.get(idx).type.compareLexicographically(corruptedVds[idx], oldV) <= 0);

        return WriteHelper.inflateInsert(schema, row.pd, row.cd, corruptedVds, null, clock.rts(row.lts[idx]));
    }
}
