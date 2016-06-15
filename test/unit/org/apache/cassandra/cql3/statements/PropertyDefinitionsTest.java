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
package org.apache.cassandra.cql3.statements;

import org.junit.After;
import org.junit.Test;
import org.junit.Before;

import static org.junit.Assert.assertEquals;

public class PropertyDefinitionsTest {
    
    PropertyDefinitions pd;
    
    @Before
    public void setUp()
    {
        pd = new PropertyDefinitions();
    }
    
    @After
    public void clear()
    {
        pd = null;
    }
    

    @Test
    public void testGetBooleanExistant()
    {
        String key = "one";
        pd.addProperty(key, "1");
        assertEquals(Boolean.TRUE, pd.getBoolean(key, null));
        
        key = "TRUE";
        pd.addProperty(key, "TrUe");
        assertEquals(Boolean.TRUE, pd.getBoolean(key, null));
        
        key = "YES";
        pd.addProperty(key, "YeS");
        assertEquals(Boolean.TRUE, pd.getBoolean(key, null));
   
        key = "BAD_ONE";
        pd.addProperty(key, " 1");
        assertEquals(Boolean.FALSE, pd.getBoolean(key, null));
        
        key = "BAD_TRUE";
        pd.addProperty(key, "true ");
        assertEquals(Boolean.FALSE, pd.getBoolean(key, null));
        
        key = "BAD_YES";
        pd.addProperty(key, "ye s");
        assertEquals(Boolean.FALSE, pd.getBoolean(key, null));
    }
    
    @Test
    public void testGetBooleanNonexistant()
    {
        assertEquals(Boolean.FALSE, pd.getBoolean("nonexistant", Boolean.FALSE));
        assertEquals(Boolean.TRUE, pd.getBoolean("nonexistant", Boolean.TRUE));
    }
    
}
