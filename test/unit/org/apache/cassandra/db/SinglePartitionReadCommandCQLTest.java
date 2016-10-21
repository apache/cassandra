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

package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import static org.junit.Assert.assertTrue;

public class SinglePartitionReadCommandCQLTest extends CQLTester
{
    @Test
    public void partitionLevelDeletionTest() throws Throwable
    {
        createTable("CREATE TABLE %s (bucket_id TEXT,name TEXT,data TEXT,PRIMARY KEY (bucket_id, name))");
        execute("insert into %s (bucket_id, name, data) values ('8772618c9009cf8f5a5e0c18', 'test', 'hello')");
        getCurrentColumnFamilyStore().forceBlockingFlush();
        execute("insert into %s (bucket_id, name, data) values ('8772618c9009cf8f5a5e0c19', 'test2', 'hello');");
        execute("delete from %s where bucket_id = '8772618c9009cf8f5a5e0c18'");
        getCurrentColumnFamilyStore().forceBlockingFlush();
        UntypedResultSet res = execute("select * from %s where bucket_id = '8772618c9009cf8f5a5e0c18' and name = 'test'");
        assertTrue(res.isEmpty());
    }
}
