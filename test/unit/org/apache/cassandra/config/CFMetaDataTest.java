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
package org.apache.cassandra.config;

import java.util.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CFMetaDataTest
{
    private static final String KEYSPACE1 = "CFMetaDataTest1";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testConversionsInverses() throws Exception
    {
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
            {
                CFMetaData cfm = cfs.metadata;
                checkInverses(cfm);

                // Testing with compression to catch #3558
                CFMetaData withCompression = cfm.copy();
                withCompression.compression(CompressionParams.snappy(32768));
                checkInverses(withCompression);
            }
        }
    }

    private void checkInverses(CFMetaData cfm) throws Exception
    {
        KeyspaceMetadata keyspace = Schema.instance.getKSMetaData(cfm.ksName);

        // Test schema conversion
        Mutation rm = SchemaKeyspace.makeCreateTableMutation(keyspace, cfm, FBUtilities.timestampMicros()).build();
        PartitionUpdate cfU = rm.getPartitionUpdate(Schema.instance.getId(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TABLES));
        PartitionUpdate cdU = rm.getPartitionUpdate(Schema.instance.getId(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.COLUMNS));

        UntypedResultSet.Row tableRow = QueryProcessor.resultify(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TABLES),
                                                                 UnfilteredRowIterators.filter(cfU.unfilteredIterator(), FBUtilities.nowInSeconds()))
                                                      .one();
        TableParams params = SchemaKeyspace.createTableParamsFromRow(tableRow);

        UntypedResultSet columnsRows = QueryProcessor.resultify(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.COLUMNS),
                                                                UnfilteredRowIterators.filter(cdU.unfilteredIterator(), FBUtilities.nowInSeconds()));
        Set<ColumnDefinition> columns = new HashSet<>();
        for (UntypedResultSet.Row row : columnsRows)
            columns.add(SchemaKeyspace.createColumnFromRow(row, Types.none()));

        assertEquals(cfm.params, params);
        assertEquals(new HashSet<>(cfm.allColumns()), columns);
    }
    
    @Test
    public void testIsNameValidPositive()
    {
         assertTrue(CFMetaData.isNameValid("abcdefghijklmnopqrstuvwxyz"));
         assertTrue(CFMetaData.isNameValid("ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
         assertTrue(CFMetaData.isNameValid("_01234567890"));
    }
    
    @Test
    public void testIsNameValidNegative()
    {
        assertFalse(CFMetaData.isNameValid(null));
        assertFalse(CFMetaData.isNameValid(""));
        assertFalse(CFMetaData.isNameValid(" "));
        assertFalse(CFMetaData.isNameValid("@"));
        assertFalse(CFMetaData.isNameValid("!"));
    }

    private static Set<String> primitiveTypes = new HashSet<String>(Arrays.asList(new String[] { "ascii", "bigint", "blob", "boolean", "date",
                                                                                                 "duration", "decimal", "double", "float",
                                                                                                 "inet", "int", "smallint", "text", "time",
                                                                                                 "timestamp", "timeuuid", "tinyint", "uuid",
                                                                                                 "varchar", "varint" }));

    @Test
    public void typeCompatibilityTest() throws Throwable
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
