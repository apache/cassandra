/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.metrics;

import java.net.InetAddress;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.google.common.collect.Iterators;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.hints.HintsService;

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;

public class HintedHandOffMetricsTest
{
    @Test
    public void testHintsMetrics() throws Exception
    {
        DatabaseDescriptor.getHintsDirectory().mkdirs();

        for (int i = 0; i < 99; i++)
            HintsService.instance.metrics.incrPastWindow(InetAddress.getLocalHost());
        HintsService.instance.metrics.log();

        UntypedResultSet rows = executeInternal("SELECT hints_dropped FROM system." + SystemKeyspace.PEER_EVENTS);
        Map<UUID, Integer> returned = rows.one().getMap("hints_dropped", UUIDType.instance, Int32Type.instance);
        assertEquals(Iterators.getLast(returned.values().iterator()).intValue(), 99);
    }
}
