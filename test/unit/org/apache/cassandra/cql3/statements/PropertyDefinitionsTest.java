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

import org.junit.Test;
import org.junit.Before;

import org.apache.cassandra.exceptions.SyntaxException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PropertyDefinitionsTest {
    
    PropertyDefinitions definitions;
    
    @Before
    public void setUp()
    {
        definitions = new PropertyDefinitions();
    }

    @Test
    public void testPostiveBooleanParsing()
    {
        for (String[] pair : new String[][] {{"prop1", "1"},
                                             {"prop2", "true"},
                                             {"prop3", "True"},
                                             {"prop4", "TrUe"},
                                             {"prop5", "yes"},
                                             {"prop6", "Yes" }})
        {
            definitions.addProperty(pair[0], pair[1]);
            assertTrue(definitions.getBoolean(pair[0], false));
        }
    }

    @Test
    public void testNegativeBooleanParsing()
    {
        for (String[] pair : new String[][] {{"prop1", "0"},
                                             {"prop2", "false"},
                                             {"prop3", "False"},
                                             {"prop4", "FaLse"},
                                             {"prop5", "no"},
                                             {"prop6", "No" }})
        {
            definitions.addProperty(pair[0], pair[1]);
            assertFalse(definitions.getBoolean(pair[0], true));
        }
    }

    @Test(expected = SyntaxException.class)
    public void testInvalidPositiveBooleanParsing()
    {
        definitions.addProperty("cdc", "tru");
        definitions.getBoolean("cdc", false);
    }

    @Test(expected = SyntaxException.class)
    public void testInvalidNegativeBooleanParsing()
    {
        definitions.addProperty("cdc", "fals");
        definitions.getBoolean("cdc", false);
    }
}
