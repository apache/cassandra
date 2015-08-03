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
package org.apache.cassandra.db.compaction;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import static org.junit.Assert.assertTrue;

public class CompactionsCQLTest extends CQLTester
{
    @Test
    public void testTriggerMinorCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY);");
        assertTrue(Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable()).getCompactionStrategy().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        Thread.sleep(1000);
        UntypedResultSet res = execute("SELECT * FROM system.compaction_history");
        boolean minorWasTriggered = false;
        for (UntypedResultSet.Row r : res)
        {
            if (r.getString("keyspace_name").equals(KEYSPACE) && r.getString("columnfamily_name").equals(currentTable()))
                minorWasTriggered = true;
        }
        assertTrue(minorWasTriggered);
    }
}
