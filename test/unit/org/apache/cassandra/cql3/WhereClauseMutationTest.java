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

import java.util.List;
import javax.annotation.Nullable;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.junit.Assert.assertEquals;

/**
 * The purpose of the test is to show the mutation of {@link WhereClause} relations
 * by a {@link QueryInterceptor}.
 *
 * The test adds 2 rows with the keys of "original" and "mutated".
 *
 * Without the {@link QueryInterceptor} when "original" is select the value associated
 * with the "original" row is returned.
 *
 * A {@link QueryInterceptor} is then added that changes the value of the primary key
 * relation from "original" to "mutated" so when "original" is selected the value for
 * the "mutated" row is returned.
 */
public class WhereClauseMutationTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Throwable
    {
        requireNetwork();
    }

    @After
    public void cleanInterceptors() throws Throwable
    {
        QueryProcessor.instance.clearInterceptors();
    }

    @Test
    public void mutationTest() throws Throwable
    {
        createTable("create table %s (pk text, ck text, v text, primary key (pk, ck))");
        execute("insert into %s (pk, ck, v) values ('original', 'clustering', 'value1')");
        execute("insert into %s (pk, ck, v) values ('mutated', 'clustering', 'value2')");

        assertRowsIgnoringOrder(execute("select * from %s where pk = 'original' and ck = 'clustering'"), row("original", "clustering", "value1"));

        QueryProcessor.instance.registerInterceptor(new QueryInterceptor()
        {
            @Nullable
            @Override
            public ResultMessage interceptStatement(CQLStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
            {
                if (statement instanceof SelectStatement)
                {
                    SelectStatement selectStatement = (SelectStatement)statement;
                    // We only want to attempt mutation on our table
                    if (selectStatement.table.keyspace.equals(keyspace()) && selectStatement.table.name.equals(currentTable()))
                    {
                        SelectStatement.RawStatement rawStatement = (SelectStatement.RawStatement)QueryProcessor.parseStatement(selectStatement.getRawCQLStatement());

                        // Mutate the SelectStatement with a new WhereClause with the relations mutated
                        rawStatement = new SelectStatement.RawStatement(new QualifiedName(rawStatement.keyspace(),
                                                                                          rawStatement.name()),
                                                                        rawStatement.parameters,
                                                                        rawStatement.selectClause,
                                                                        rawStatement.whereClause.mutateRelations(r -> mutateRelation(r)),
                                                                        rawStatement.limit,
                                                                        rawStatement.perPartitionLimit);

                        selectStatement = rawStatement.prepare(queryState.getClientState());
                        return selectStatement.execute(queryState, options, queryStartNanoTime);
                    }
                }
                return null;
            }
        });

        List<Row> rows = executeNet("select * from %s where pk = 'original' and ck = 'clustering'").all();
        assertEquals(1, rows.size());
        assertEquals("value2", rows.get(0).getString(2));
    }

    private Relation mutateRelation(Relation original)
    {
        // Only perform the mutation on single column relations
        if (original instanceof SingleColumnRelation)
        {
            SingleColumnRelation singleColumnRelation = (SingleColumnRelation)original;
            // Make sure we are only changing the primary key column
            if (singleColumnRelation.getEntity().toCQLString().equals("pk"))
                // Return a new SingleColumnRelation with the primary key column
                // value changed to "mutated"
                return new SingleColumnRelation(singleColumnRelation.getEntity(),
                                                singleColumnRelation.operator(),
                                                Constants.Literal.string("mutated"));
        }
        return original;
    }
}
