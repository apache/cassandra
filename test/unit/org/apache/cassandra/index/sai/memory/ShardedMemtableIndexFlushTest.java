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

package org.apache.cassandra.index.sai.memory;

import org.junit.Test;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;

public class ShardedMemtableIndexFlushTest extends SAIRandomizedTester
{
    @Test
    public void flushShardedIndexWithReversedClustering()
    {
        createTable("CREATE TABLE %s (pk0 varint, ck0 bigint, ck1 text, v0 uuid, PRIMARY KEY (pk0, ck0, ck1)) WITH CLUSTERING ORDER BY (ck0 DESC, ck1 DESC)");

        createIndex("CREATE CUSTOM INDEX ON %s(ck0) USING 'StorageAttachedIndex' WITH OPTIONS = {'shards': '4'}");
        createIndex("CREATE CUSTOM INDEX ON %s(ck1) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk0, ck0, ck1, v0) VALUES (0, 3796795943589367651, 'apple', uuid()) USING TIMESTAMP 2");
        execute("INSERT INTO %s (pk0, ck0, ck1, v0) VALUES (0, 5390896686766209114, 'banana', uuid()) USING TIMESTAMP 4");

        execute("UPDATE %s USING TIMESTAMP 5 SET v0 = uuid() WHERE pk0 = 13271891 AND ck0 = 7466188966332780245 AND ck1 = 'cherry'");
        execute("UPDATE %s USING TIMESTAMP 5 SET v0 = uuid() WHERE pk0 = 2081887771 AND ck0 = 7466188966332780245 AND ck1 = 'cherry'");

        execute("UPDATE %s USING TIMESTAMP 6 SET v0 = uuid() WHERE pk0 = 0 AND ck0 = 8977613850235134686 AND ck1 = 'date'");
        execute("UPDATE %s USING TIMESTAMP 6 SET v0 = uuid() WHERE pk0 = 4982 AND ck0 = 8977613850235134686 AND ck1 = 'date'");

        // Verify data is visible before flush
        UntypedResultSet beforeFlush = execute("SELECT * FROM %s WHERE ck0 = 8977613850235134686 AND ck1 = 'date' ALLOW FILTERING");
        assertRowCount(beforeFlush, 2);

        flush();

        // After flush, same query should return same results
        UntypedResultSet afterFlush = execute("SELECT * FROM %s WHERE ck0 = 8977613850235134686 AND ck1 = 'date' ALLOW FILTERING");
        assertRowCount(afterFlush, 2);
    }
}