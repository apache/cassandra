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
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.ThriftConversion;
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

    private static List<ColumnDef> columnDefs = new ArrayList<ColumnDef>();

    static
    {
        columnDefs.add(new ColumnDef(ByteBufferUtil.bytes("col1"), AsciiType.class.getCanonicalName())
                                    .setIndex_name("col1Index")
                                    .setIndex_type(IndexType.KEYS));

        columnDefs.add(new ColumnDef(ByteBufferUtil.bytes("col2"), UTF8Type.class.getCanonicalName())
                                    .setIndex_name("col2Index")
                                    .setIndex_type(IndexType.KEYS));

        Map<String, String> customIndexOptions = new HashMap<>();
        customIndexOptions.put("option1", "value1");
        customIndexOptions.put("option2", "value2");
        columnDefs.add(new ColumnDef(ByteBufferUtil.bytes("col3"), Int32Type.class.getCanonicalName())
                                    .setIndex_name("col3Index")
                                    .setIndex_type(IndexType.CUSTOM)
                                    .setIndex_options(customIndexOptions));
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testThriftConversion() throws Exception
    {
        CfDef cfDef = new CfDef().setDefault_validation_class(AsciiType.class.getCanonicalName())
                                 .setComment("Test comment")
                                 .setColumn_metadata(columnDefs)
                                 .setKeyspace(KEYSPACE1)
                                 .setName(CF_STANDARD1);

        // convert Thrift to CFMetaData
        CFMetaData cfMetaData = ThriftConversion.fromThrift(cfDef);

        CfDef thriftCfDef = new CfDef();
        thriftCfDef.keyspace = KEYSPACE1;
        thriftCfDef.name = CF_STANDARD1;
        thriftCfDef.default_validation_class = cfDef.default_validation_class;
        thriftCfDef.comment = cfDef.comment;
        thriftCfDef.column_metadata = new ArrayList<>();
        for (ColumnDef columnDef : columnDefs)
        {
            ColumnDef c = new ColumnDef();
            c.name = ByteBufferUtil.clone(columnDef.name);
            c.validation_class = columnDef.getValidation_class();
            c.index_name = columnDef.getIndex_name();
            c.index_type = columnDef.getIndex_type();
            if (columnDef.isSetIndex_options())
                c.setIndex_options(columnDef.getIndex_options());
            thriftCfDef.column_metadata.add(c);
        }

        CfDef converted = ThriftConversion.toThrift(cfMetaData);

        assertEquals(thriftCfDef.keyspace, converted.keyspace);
        assertEquals(thriftCfDef.name, converted.name);
        assertEquals(thriftCfDef.default_validation_class, converted.default_validation_class);
        assertEquals(thriftCfDef.comment, converted.comment);
        assertEquals(new HashSet<>(thriftCfDef.column_metadata), new HashSet<>(converted.column_metadata));
    }

    @Test
    public void testConversionsInverses() throws Exception
    {
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
            {
                CFMetaData cfm = cfs.metadata;
                if (!cfm.isThriftCompatible())
                    continue;

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

        // Test thrift conversion
        CFMetaData before = cfm;
        CFMetaData after = ThriftConversion.fromThriftForUpdate(ThriftConversion.toThrift(before), before);
        assert before.equals(after) : String.format("%n%s%n!=%n%s", before, after);

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
