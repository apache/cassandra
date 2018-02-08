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

package org.apache.cassandra.schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cdc.ICDCHandler;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CDCParamsTest
{
    @Test
    public void testDefaultParams()
    {
        CDCParams params = CDCParams.fromMap(null);
        assertTrue(params.isDefaultHandler());

        params = CDCParams.fromMap(new HashMap<>());
        assertTrue(params.isDefaultHandler());
    }

    @Test
    public void testUnknownClass()
    {
        Map values = new HashMap<>();
        values.put("class", "org.dummy");
        CDCParams params = CDCParams.fromMap(values);
        assertTrue(params.isUnknownHandler());
        assertTrue(params.isNoOpsHandler());
        values = params.asMap();
        assertEquals("org.dummy", values.get(CDCParams.Option.CLASS.toString()));
    }

    @Test
    public void testNormalClass()
    {
        Map values = new HashMap<>();
        values.put("class", "org.apache.cassandra.schema.CDCParamsTest$TestHandler");
        CDCParams params = CDCParams.fromMap(values);
        assertFalse(params.isNoOpsHandler());
        values = params.asMap();
        assertEquals("org.apache.cassandra.schema.CDCParamsTest$TestHandler", values.get(CDCParams.Option.CLASS.toString()));
    }

    @Test
    public void testOptions()
    {
        Map values = new HashMap<>();
        values.put("class", "org.apache.cassandra.schema.CDCParamsTest$TestHandler");
        values.put("option1", "test1");
        values.put("option2", "test2");
        CDCParams params = CDCParams.fromMap(values);
        assertFalse(params.isNoOpsHandler());
        values = params.asMap();
        assertEquals("org.apache.cassandra.schema.CDCParamsTest$TestHandler", values.get(CDCParams.Option.CLASS.toString()));
        assertEquals("test1", values.get("option1"));
        assertEquals("test2", values.get("option2"));
    }

    public class TestHandler implements ICDCHandler
    {
        public void initialize(Map<String, String> options) throws ConfigurationException
        {
        }
        public void process(Mutation mutation) throws IOException
        {
        }
    }
}
