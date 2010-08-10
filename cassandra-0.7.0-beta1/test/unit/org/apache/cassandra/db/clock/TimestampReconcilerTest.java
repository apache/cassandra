/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.clock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.TimestampClock;
import org.apache.cassandra.db.clock.TimestampReconciler;
import org.junit.Test;

public class TimestampReconcilerTest
{   
    private static final TimestampReconciler reconciler = new TimestampReconciler();

    @Test
    public void testReconcileNormal()
    {
        TimestampClock leftClock = new TimestampClock(1);
        TimestampClock rightClock = new TimestampClock(2);

        Column left = new Column(
                "x".getBytes(),
                new byte[] {},
                leftClock);
        Column right = new Column(
                "x".getBytes(),
                new byte[] {},
                rightClock);
        
        Column reconciled = reconciler.reconcile(left, right);

        assertFalse(reconciled.isMarkedForDelete());
        assertEquals(reconciled, right);
    }

    @Test
    public void testReconcileDeleted()
    {
        TimestampClock leftClock = new TimestampClock(2);
        TimestampClock rightClock = new TimestampClock(1);

        Column left = new DeletedColumn(
                "x".getBytes(),
                new byte[] {},
                leftClock);
        Column right = new Column(
                "x".getBytes(),
                new byte[] {},
                rightClock);
        
        Column reconciled = reconciler.reconcile(left, right);

        assertTrue(reconciled.isMarkedForDelete());
        assertEquals(reconciled, left);
    }
}
