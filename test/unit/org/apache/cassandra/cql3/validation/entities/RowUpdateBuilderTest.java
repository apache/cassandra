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
package org.apache.cassandra.cql3.validation.entities;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.utils.FBUtilities;

// see CASSANDRA-9743, CASSANDRA-9746
public class RowUpdateBuilderTest extends CQLTester
{
    @Test
    public void testAddListEntryDurable() throws Throwable
    {
        testAddListEntry(false);
    }

    @Test
    public void testAddListEntryTransient() throws Throwable
    {
        testAddListEntry(true);
    }

    public void testAddListEntry(boolean skipCommitLog) throws Throwable
    {
        createTable("CREATE TABLE %s ("
                    + "pk text,"
                    + "ck text,"
                    + "l1 list<int>,"
                    + "l2 list<int>,"
                    + "PRIMARY KEY ((pk), ck))");

        long timestamp = FBUtilities.timestampMicros();

        Mutation mutation = new Mutation(keyspace(), Util.dk("test"));
        addToMutation("row1", timestamp, mutation);
        addToMutation("row2", timestamp, mutation);

        if (skipCommitLog)
            mutation.applyUnsafe();
        else
            mutation.apply();

        assertRowCount(execute("SELECT ck FROM %s"), 2);
    }

    private void addToMutation(String typeName, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(getCurrentColumnFamilyStore().metadata, timestamp, mutation)
                                 .clustering(typeName);

        for (int i = 0; i < 2; i++)
        {
            adder.addListEntry("l1", i)
                 .addListEntry("l2", i);
        }

        adder.build();
    }
}