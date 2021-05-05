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

import static org.apache.cassandra.db.memtable.TrieMemtable.TRIE_MEMTABLE_CONFIG_OBJECT_NAME;
import static org.junit.Assert.assertEquals;

public class TrieMemtableConfigTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        createMBeanServerConnection();
    }

    @Test
    public void testShardCountSetByJMX() throws MalformedObjectNameException, ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, IOException, InvalidAttributeValueException
    {
        jmxConnection.setAttribute(new ObjectName(TRIE_MEMTABLE_CONFIG_OBJECT_NAME), new Attribute("ShardCount", "7"));
        assertEquals(7, TrieMemtable.getShardCount());
    }

    @Test
    public void testAutoShardCount() throws MalformedObjectNameException, ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, IOException, InvalidAttributeValueException
    {
        jmxConnection.setAttribute(new ObjectName(TRIE_MEMTABLE_CONFIG_OBJECT_NAME), new Attribute("ShardCount", "auto"));
        assertEquals(FBUtilities.getAvailableProcessors(), TrieMemtable.getShardCount());
    }
}