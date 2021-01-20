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

package org.apache.cassandra.db.memtable;

import java.io.IOException;
import javax.management.Attribute;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.memtable.AbstractShardedMemtable.SHARDED_MEMTABLE_CONFIG_OBJECT_NAME;
import static org.junit.Assert.assertEquals;

public class ShardedMemtableConfigTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        createMBeanServerConnection();
    }

    @Test
    public void testDefaultShardCountSetByJMX() throws MalformedObjectNameException, ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, IOException, InvalidAttributeValueException, InterruptedException
    {
        // check the default, but also make sure the class is initialized if the default memtable is not sharded
        assertEquals(FBUtilities.getAvailableProcessors(), AbstractShardedMemtable.getDefaultShardCount());
        jmxConnection.setAttribute(new ObjectName(SHARDED_MEMTABLE_CONFIG_OBJECT_NAME), new Attribute("DefaultShardCount", "7"));
        assertEquals(7, AbstractShardedMemtable.getDefaultShardCount());
        assertEquals("7", jmxConnection.getAttribute(new ObjectName(SHARDED_MEMTABLE_CONFIG_OBJECT_NAME), "DefaultShardCount"));
    }

    @Test
    public void testAutoShardCount() throws MalformedObjectNameException, ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, IOException, InvalidAttributeValueException
    {
        AbstractShardedMemtable.getDefaultShardCount();    // initialize class
        jmxConnection.setAttribute(new ObjectName(SHARDED_MEMTABLE_CONFIG_OBJECT_NAME), new Attribute("DefaultShardCount", "auto"));
        assertEquals(FBUtilities.getAvailableProcessors(), AbstractShardedMemtable.getDefaultShardCount());
        assertEquals(Integer.toString(FBUtilities.getAvailableProcessors()),
                     jmxConnection.getAttribute(new ObjectName(SHARDED_MEMTABLE_CONFIG_OBJECT_NAME), "DefaultShardCount"));
    }
}
