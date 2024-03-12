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

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ViewDirectModificationTest extends ViewAbstractTest
{
    @After
    public void disableDirectViewModification() {
        StorageService.instance.setDirectMaterializedViewModification(false);
    }

    @Test
    public void testDirectModification() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int, c2 int, c3 int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet("USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW %s AS SELECT k1, c1, c2, c3 FROM %s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL PRIMARY KEY (c2, k1, c1)");

        Assert.assertEquals(0, execute("select * from view1").size());
        Assert.assertEquals(0, ClientRequestsMetricsHolder.viewWriteMetrics.directMVModification.getCount());
        assertFalse(StorageService.instance.getDirectMaterializedViewModificationEnabled());
        try
        {
            updateView("INSERT INTO view1 (k1, c1, c2, c3) VALUES (1, 2, 3, 4)");
            fail("a direct insert to MV should fail");
        }
        catch (Exception e) {
            assertEquals("Cannot directly modify a materialized view", e.getMessage());
        }
        try
        {
            updateView("UPDATE view1 SET c3=40 WHERE c2 = 3 and k1 = 1 and c1 = 2");
            fail("a direct update to MV should fail");
        }
        catch (Exception e) {
            assertEquals("Cannot directly modify a materialized view", e.getMessage());
        }
        try
        {
            updateView("DELETE FROM view1 WHERE c2 = 3");
            fail("a direct delete to MV should fail");
        }
        catch (Exception e) {
            assertEquals("Cannot directly modify a materialized view", e.getMessage());
        }
        Assert.assertEquals(0, execute("select * from view1").size());
        Assert.assertEquals(0, ClientRequestsMetricsHolder.viewWriteMetrics.directMVModification.getCount());

        StorageService.instance.setDirectMaterializedViewModification(true);
        assertTrue(StorageService.instance.getDirectMaterializedViewModificationEnabled());

        updateView("INSERT INTO view1 (k1, c1, c2, c3) VALUES (1, 2, 3, 4)");
        Assert.assertEquals(1, execute("select * from view1").size());
        Assert.assertEquals(1, ClientRequestsMetricsHolder.viewWriteMetrics.directMVModification.getCount());

        updateView("UPDATE view1 SET c3=40 WHERE c2 = 3 and k1 = 1 and c1 = 2");
        Assert.assertEquals(1, execute("select * from view1").size());
        Assert.assertEquals(2, ClientRequestsMetricsHolder.viewWriteMetrics.directMVModification.getCount());

        updateView("DELETE FROM view1 WHERE c2 = 3");
        Assert.assertEquals(0, execute("select * from view1").size());
        Assert.assertEquals(3, ClientRequestsMetricsHolder.viewWriteMetrics.directMVModification.getCount());
    }
}
