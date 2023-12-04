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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

public class MapEntriesIndexInvalidQueryTest extends SAITester
{

    @Test
    public void testConflictingBounds() throws Throwable
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // Invalid queries
        assertInvalidMessage("More than one restriction was found for the end bound on item_cost",
                             "SELECT partition FROM %s WHERE item_cost['apple'] <= 6 AND item_cost['apple'] < 10");
        assertInvalidMessage("More than one restriction was found for the start bound on item_cost",
                             "SELECT partition FROM %s WHERE item_cost['apple'] > 0 AND item_cost['apple'] > 1");
        assertInvalidMessage("Column \"item_cost\" cannot be restricted by both an inequality " +
                             "relation and \"CONTAINS(values=[], keys=[], entryKeys=[6170706c65], entryValues=[00000001])\"",
                             "SELECT partition FROM %s WHERE item_cost['apple'] > 0 AND item_cost['apple'] = 1");
    }

}
