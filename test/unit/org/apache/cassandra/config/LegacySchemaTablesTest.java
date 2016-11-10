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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;

import com.google.common.collect.Iterables;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.*;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LegacySchemaTablesTest
{
    private static final String KEYSPACE1 = "CFMetaDataTest1";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STANDARD2 = "Standard2";

    private static List<ColumnDef> columnDefs = new ArrayList<ColumnDef>();

    static
    {
        columnDefs.add(new ColumnDef(ByteBufferUtil.bytes("col1"), AsciiType.class.getCanonicalName())
                                    .setIndex_name("col1Index")
                                    .setIndex_type(IndexType.KEYS));

        columnDefs.add(new ColumnDef(ByteBufferUtil.bytes("col2"), UTF8Type.class.getCanonicalName())
                                    .setIndex_name("col2Index")
                                    .setIndex_type(IndexType.KEYS));
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testIsDenseRecalculation()
    {
        // 1.a start with a dense CF
        CfDef cfDef0 = new CfDef().setDefault_validation_class(BytesType.class.getCanonicalName())
                                  .setComparator_type(UTF8Type.class.getCanonicalName())
                                  .setColumn_metadata(Collections.<ColumnDef>emptyList())
                                  .setKeyspace(KEYSPACE1)
                                  .setName(CF_STANDARD2);
        CFMetaData cfm0 = ThriftConversion.fromThrift(cfDef0);
        MigrationManager.announceNewColumnFamily(cfm0, true);

        // 1.b validate that the cf is dense, has a single compact value and a clustering column, and no regulars
        CFMetaData current = Schema.instance.getCFMetaData(KEYSPACE1, CF_STANDARD2);
        assertTrue(current.getIsDense());
        assertNotNull(current.compactValueColumn());
        assertEquals(0, Iterables.size(current.regularAndStaticColumns()));
        assertEquals(1, current.clusteringColumns().size());

        // 2.a add a column to the table
        CfDef cfDef1 = ThriftConversion.toThrift(current);
        List<ColumnDef> colDefs =
            Collections.singletonList(new ColumnDef(ByteBufferUtil.bytes("col1"), AsciiType.class.getCanonicalName()));
        cfDef1.setColumn_metadata(colDefs);
        CFMetaData cfm1 = ThriftConversion.fromThriftForUpdate(cfDef1, current);
        MigrationManager.announceColumnFamilyUpdate(cfm1, true);

        // 2.b validate that the cf is sparse now, had no compact value column or clustering column, and 1 regular
        current = Schema.instance.getCFMetaData(KEYSPACE1, CF_STANDARD2);
        assertFalse(current.getIsDense());
        assertNull(current.compactValueColumn());
        assertEquals(1, Iterables.size(current.regularAndStaticColumns()));
        assertEquals(0, current.clusteringColumns().size());

        // 3.a remove the column
        CfDef cfDef2 = ThriftConversion.toThrift(current);
        cfDef2.setColumn_metadata(Collections.<ColumnDef>emptyList());
        CFMetaData cfm2 = ThriftConversion.fromThriftForUpdate(cfDef2, current);
        MigrationManager.announceColumnFamilyUpdate(cfm2, true);

        // 3.b validate that the cf is dense, has a single compact value and a clustering column, and no regulars
        current = Schema.instance.getCFMetaData(KEYSPACE1, CF_STANDARD2);
        assertTrue(current.getIsDense());
        assertNotNull(current.compactValueColumn());
        assertEquals(0, Iterables.size(current.regularAndStaticColumns()));
        assertEquals(1, current.clusteringColumns().size());
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
            c.index_type = IndexType.KEYS;
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
                withCompression.compressionParameters(new CompressionParameters(SnappyCompressor.instance, 32768, new HashMap<String, String>()));
                checkInverses(withCompression);
            }
        }
    }

    private void checkInverses(CFMetaData cfm) throws Exception
    {
        DecoratedKey k = StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(cfm.ksName));
        KSMetaData keyspace = Schema.instance.getKSMetaData(cfm.ksName);

        // Test thrift conversion
        CFMetaData before = cfm;
        CFMetaData after = ThriftConversion.fromThriftForUpdate(ThriftConversion.toThrift(before), before);
        assert before.equals(after) : String.format("%n%s%n!=%n%s", before, after);

        // Test schema conversion
        Mutation rm = LegacySchemaTables.makeCreateTableMutation(keyspace, cfm, FBUtilities.timestampMicros());
        ColumnFamily serializedCf = rm.getColumnFamily(Schema.instance.getId(SystemKeyspace.NAME, LegacySchemaTables.COLUMNFAMILIES));
        ColumnFamily serializedCD = rm.getColumnFamily(Schema.instance.getId(SystemKeyspace.NAME, LegacySchemaTables.COLUMNS));
        CFMetaData newCfm = LegacySchemaTables.createTableFromTablePartitionAndColumnsPartition(new Row(k, serializedCf), new Row(k, serializedCD));
        assert cfm.equals(newCfm) : String.format("%n%s%n!=%n%s", cfm, newCfm);
    }
}
