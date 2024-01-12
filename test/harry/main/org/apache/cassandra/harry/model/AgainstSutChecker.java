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

package org.apache.cassandra.harry.model;

import java.util.List;

import org.apache.cassandra.harry.data.ResultSetRow;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.sut.QueryModifyingSut;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.tracker.DataTracker;

/**
 * A simple way to verify if something might be a Harry issue: check against a different SUT.
 *
 * For example, if you are using `flush` in your primary SUT, avoid using it for the secondary SUT,
 * and compare results.
 *
 * Usually used in combination with {@link QueryModifyingSut}, which writes to
 * the second SUT.
 *
 *             SchemaSpec doubleWriteSchema = schema.cloneWithName(schema.keyspace, schema.keyspace + "_debug");
 *
 *             sut.schemaChange(doubleWriteSchema.compile().cql());
 *
 *             QueryModifyingSut sut = new QueryModifyingSut(this.sut,
 *                                                           schema.table,
 *                                                           doubleWriteSchema.table);
 *
 *
 *             Model model = new AgainstSutChecker(tracker, history.clock(), sut, schema, doubleWriteSchema);
 */
public class AgainstSutChecker implements Model
{
    protected final OpSelectors.Clock clock;
    protected final SystemUnderTest sut;
    protected final SchemaSpec schema;
    protected final SchemaSpec doubleWriteTable;
    protected final DataTracker tracker;

    public AgainstSutChecker(DataTracker tracker,
                             OpSelectors.Clock clock,
                             SystemUnderTest sut,
                             SchemaSpec schema,
                             SchemaSpec doubleWriteTable)
    {
        this.clock = clock;
        this.sut = sut;
        this.schema = schema;
        this.doubleWriteTable = doubleWriteTable;
        this.tracker = tracker;
    }

    public void validate(Query query)
    {
        tracker.beginValidation(query.pd);

        List<ResultSetRow> rows1 = executeOnMainSchema(query);
        List<ResultSetRow> rows2 = executeOnDebugSchema(query);

        if (rows1.size() != rows2.size())
            throw new IllegalStateException(String.format("Sizes do not match %d %d\n%s\n%s\nQuery:%s\n", rows1.size(), rows2.size(), rows1, rows2, query.toSelectStatement()));

        for (int i = 0; i < rows1.size(); i++)
        {
            if (!rows1.get(i).equals(rows2.get(i)))
            {
                throw new IllegalStateException(String.format("Rows mismatch:\n" +
                                                              "%s\n" +
                                                              "%s\n",
                                                              rows1.get(i),
                                                              rows2.get(i)));
            }
        }
        tracker.endValidation(query.pd);
    }

    protected final List<ResultSetRow> executeOnMainSchema(Query query)
    {
        CompiledStatement s1 = query.toSelectStatement(schema.allColumnsSet, true);
        return SelectHelper.execute(sut, clock, s1, schema);
    }

    protected List<ResultSetRow> executeOnDebugSchema(Query query)
    {
        CompiledStatement s2 = query.toSelectStatement(doubleWriteTable.allColumnsSet, true)
                                    .withSchema(schema.keyspace, schema.table, doubleWriteTable.keyspace, doubleWriteTable.table);
        return SelectHelper.execute(sut, clock, s2, schema);
    }

}
