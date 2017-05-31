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
package org.apache.cassandra.schema;

import java.util.*;

import org.apache.cassandra.db.marshal.*;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidationTest
{
    @Test
    public void testIsNameValidPositive()
    {
         assertTrue(SchemaConstants.isValidName("abcdefghijklmnopqrstuvwxyz"));
         assertTrue(SchemaConstants.isValidName("ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
         assertTrue(SchemaConstants.isValidName("_01234567890"));
    }
    
    @Test
    public void testIsNameValidNegative()
    {
        assertFalse(SchemaConstants.isValidName(null));
        assertFalse(SchemaConstants.isValidName(""));
        assertFalse(SchemaConstants.isValidName(" "));
        assertFalse(SchemaConstants.isValidName("@"));
        assertFalse(SchemaConstants.isValidName("!"));
    }

    private static Set<String> primitiveTypes =
        new HashSet<>(Arrays.asList(new String[] { "ascii", "bigint", "blob", "boolean", "date",
                                                   "duration", "decimal", "double", "float",
                                                   "inet", "int", "smallint", "text", "time",
                                                   "timestamp", "timeuuid", "tinyint", "uuid",
                                                   "varchar", "varint" }));

    @Test
    public void typeCompatibilityTest()
    {
        Map<String, Set<String>> compatibilityMap = new HashMap<>();
        compatibilityMap.put("bigint", new HashSet<>(Arrays.asList(new String[] {"timestamp"})));
        compatibilityMap.put("blob", new HashSet<>(Arrays.asList(new String[] {"ascii", "bigint", "boolean", "date", "decimal", "double", "duration",
                                                                               "float", "inet", "int", "smallint", "text", "time", "timestamp",
                                                                               "timeuuid", "tinyint", "uuid", "varchar", "varint"})));
        compatibilityMap.put("date", new HashSet<>(Arrays.asList(new String[] {"int"})));
        compatibilityMap.put("time", new HashSet<>(Arrays.asList(new String[] {"bigint"})));
        compatibilityMap.put("text", new HashSet<>(Arrays.asList(new String[] {"ascii", "varchar"})));
        compatibilityMap.put("timestamp", new HashSet<>(Arrays.asList(new String[] {"bigint"})));
        compatibilityMap.put("varchar", new HashSet<>(Arrays.asList(new String[] {"ascii", "text"})));
        compatibilityMap.put("varint", new HashSet<>(Arrays.asList(new String[] {"bigint", "int", "timestamp"})));
        compatibilityMap.put("uuid", new HashSet<>(Arrays.asList(new String[] {"timeuuid"})));

        for (String sourceTypeString: primitiveTypes)
        {
            AbstractType sourceType = CQLTypeParser.parse("KEYSPACE", sourceTypeString, Types.none());
            for (String destinationTypeString: primitiveTypes)
            {
                AbstractType destinationType = CQLTypeParser.parse("KEYSPACE", destinationTypeString, Types.none());

                if (compatibilityMap.get(destinationTypeString) != null &&
                    compatibilityMap.get(destinationTypeString).contains(sourceTypeString) ||
                    sourceTypeString.equals(destinationTypeString))
                {
                    assertTrue(sourceTypeString + " should be compatible with " + destinationTypeString,
                               destinationType.isValueCompatibleWith(sourceType));
                }
                else
                {
                    assertFalse(sourceTypeString + " should not be compatible with " + destinationTypeString,
                                destinationType.isValueCompatibleWith(sourceType));
                }
            }
        }
    }

    @Test
    public void clusteringColumnTypeCompatibilityTest() throws Throwable
    {
        Map<String, Set<String>> compatibilityMap = new HashMap<>();
        compatibilityMap.put("blob", new HashSet<>(Arrays.asList(new String[] {"ascii", "text", "varchar"})));
        compatibilityMap.put("text", new HashSet<>(Arrays.asList(new String[] {"ascii", "varchar"})));
        compatibilityMap.put("varchar", new HashSet<>(Arrays.asList(new String[] {"ascii", "text" })));

        for (String sourceTypeString: primitiveTypes)
        {
            AbstractType sourceType = CQLTypeParser.parse("KEYSPACE", sourceTypeString, Types.none());
            for (String destinationTypeString: primitiveTypes)
            {
                AbstractType destinationType = CQLTypeParser.parse("KEYSPACE", destinationTypeString, Types.none());

                if (compatibilityMap.get(destinationTypeString) != null &&
                    compatibilityMap.get(destinationTypeString).contains(sourceTypeString) ||
                    sourceTypeString.equals(destinationTypeString))
                {
                    assertTrue(sourceTypeString + " should be compatible with " + destinationTypeString,
                               destinationType.isCompatibleWith(sourceType));
                }
                else
                {
                    assertFalse(sourceTypeString + " should not be compatible with " + destinationTypeString,
                                destinationType.isCompatibleWith(sourceType));
                }
            }
        }
    }
}
