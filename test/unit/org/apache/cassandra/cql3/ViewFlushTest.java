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

import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

public class ViewFlushTest extends ViewAbstractTest
{

    @Test
    public void testView() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, a int, b int, c int, primary key(k, a)) with default_time_to_live=6000");
        createView("CREATE MATERIALIZED VIEW %s AS SELECT k,a,b FROM %s WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k)");

        execute("UPDATE %s SET c=2 WHERE k=1 AND a=1");
        flushView();
        assertRows(execute("SELECT k,a,b,c FROM %s"), row(1, 1, null, 2));
        assertRows(executeView("SELECT k,a,b FROM %s"), row(1, 1, null));

        // view mutation on update
        //  [cql_test_keyspace.mv_testview_01] key=1 partition_deletion=LIVE columns=[[] | [b]]
        //    Row[info=[ts=1770378054212000 ttl=6000, let=1770384054] ]: k=1 |

        // view mutation on delete
        // [cql_test_keyspace.mv_testview_01] key=1 partition_deletion=LIVE columns=[[] | [b]]
        //    Row[info=[ts=1770377809808000 ttl=2147483647, let=1770377924] ]: k=1 |

        execute("DELETE c FROM %s WHERE k=1 AND a=1");
        flushView();

        assertRows(execute("SELECT k,a,b,c FROM %s"), Collections.emptyList());
        assertRows(executeView("SELECT k,a,b FROM %s"), Collections.emptyList());

        compact(keyspace(), currentView());

        assertRows(execute("SELECT k,a,b,c FROM %s"), Collections.emptyList());
        assertRows(executeView("SELECT k,a,b FROM %s"), Collections.emptyList());
    }

    private void flushView()
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentView());
        Util.flush(cfs);
    }
}