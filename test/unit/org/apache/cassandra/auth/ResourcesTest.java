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

package org.apache.cassandra.auth;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;

import static org.junit.Assert.assertEquals;

public class ResourcesTest
{

    @Test
    public void testRoleResourceNameConversion()
    {
        assertEquals(RoleResource.root(), Resources.fromName("roles"));
        assertEquals("roles", RoleResource.root().getName());

        assertEquals(RoleResource.role("role1"), Resources.fromName("roles/role1"));
        assertEquals("roles/role1", RoleResource.role("role1").getName());
    }

    @Test
    public void testDataResourceNameConversion()
    {
        assertEquals(DataResource.root(), Resources.fromName("data"));
        assertEquals("data", DataResource.root().getName());

        assertEquals(DataResource.keyspace("ks1"), Resources.fromName("data/ks1"));
        assertEquals("data/ks1", DataResource.keyspace("ks1").getName());

        assertEquals(DataResource.allTables("ks1"), Resources.fromName("data/ks1/*"));
        assertEquals("data/ks1/*", DataResource.allTables("ks1").getName());

        assertEquals(DataResource.table("ks1", "t1"), Resources.fromName("data/ks1/t1"));
        assertEquals("data/ks1/t1", DataResource.table("ks1", "t1").getName());
    }

    @Test
    public void testFunctionResourceNameConversion()
    {
        assertEquals(FunctionResource.root(), Resources.fromName("functions"));
        assertEquals("functions", FunctionResource.root().getName());

        assertEquals(FunctionResource.keyspace("ks1"), Resources.fromName("functions/ks1"));
        assertEquals("functions/ks1", FunctionResource.keyspace("ks1").getName());

        assertEquals(FunctionResource.function("ks1", "f1", Collections.emptyList()),
                Resources.fromName("functions/ks1/f1[]"));
        // this is actually supported by an explicit check in TypeParser
        assertEquals(FunctionResource.function("ks1", "f1", Arrays.asList(Int32Type.instance, DoubleType.instance)),
                Resources.fromName("functions/ks1/f1[Int32Type^DoubleType]"));
        assertEquals("functions/ks1/f1[]",
                FunctionResource.function("ks1", "f1", Collections.emptyList()).getName());

        assertEquals(FunctionResource.function("ks1", "f1", Arrays.asList(Int32Type.instance, DoubleType.instance)),
                Resources.fromName("functions/ks1/f1[org.apache.cassandra.db.marshal.Int32Type^org.apache.cassandra.db.marshal.DoubleType]"));
        assertEquals("functions/ks1/f1[org.apache.cassandra.db.marshal.Int32Type^org.apache.cassandra.db.marshal.DoubleType]",
                FunctionResource.function("ks1", "f1", Arrays.asList(Int32Type.instance, DoubleType.instance)).getName());
    }

    @Test
    public void testJMXResourceNameConversion()
    {
        assertEquals(JMXResource.root(), Resources.fromName("mbean"));
        assertEquals("mbean", JMXResource.root().getName());

        assertEquals(JMXResource.mbean("org.apache.cassandra.auth:type=*"),
                Resources.fromName("mbean/org.apache.cassandra.auth:type=*"));
        assertEquals("mbean/org.apache.cassandra.auth:type=*",
                JMXResource.mbean("org.apache.cassandra.auth:type=*").getName());
    }
}
