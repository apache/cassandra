/**
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
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.compress.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class CFMetaDataTest extends SchemaLoader
{
    private static String KEYSPACE = "Keyspace1";
    private static String COLUMN_FAMILY = "Standard1";

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

    @Test
    public void testThriftConversion() throws Exception
    {
        CfDef cfDef = new CfDef().setDefault_validation_class(AsciiType.class.getCanonicalName())
                                 .setComment("Test comment")
                                 .setColumn_metadata(columnDefs)
                                 .setKeyspace(KEYSPACE)
                                 .setName(COLUMN_FAMILY);

        // convert Thrift to CFMetaData
        CFMetaData cfMetaData = CFMetaData.fromThrift(cfDef);

        CfDef thriftCfDef = new CfDef();
        thriftCfDef.keyspace = KEYSPACE;
        thriftCfDef.name = COLUMN_FAMILY;
        thriftCfDef.default_validation_class = cfDef.default_validation_class;
        thriftCfDef.comment = cfDef.comment;
        thriftCfDef.column_metadata = new ArrayList<ColumnDef>();
        for (ColumnDef columnDef : columnDefs)
        {
            ColumnDef c = new ColumnDef();
            c.name = ByteBufferUtil.clone(columnDef.name);
            c.validation_class = columnDef.getValidation_class();
            c.index_name = columnDef.getIndex_name();
            c.index_type = IndexType.KEYS;
            thriftCfDef.column_metadata.add(c);
        }

        CfDef converted = cfMetaData.toThrift();

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

        // Test thrift conversion
        CFMetaData before = cfm;
        CFMetaData after = CFMetaData.fromThriftForUpdate(before.toThrift(), before);
        assert before.equals(after) : String.format("%n%s%n!=%n%s", before, after);

        // Test schema conversion
        Mutation rm = cfm.toSchema(System.currentTimeMillis());
        ColumnFamily serializedCf = rm.getColumnFamily(Schema.instance.getId(Keyspace.SYSTEM_KS, SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF));
        ColumnFamily serializedCD = rm.getColumnFamily(Schema.instance.getId(Keyspace.SYSTEM_KS, SystemKeyspace.SCHEMA_COLUMNS_CF));
        UntypedResultSet.Row result = QueryProcessor.resultify("SELECT * FROM system.schema_columnfamilies", new Row(k, serializedCf)).one();
        CFMetaData newCfm = CFMetaData.fromSchemaNoTriggers(result, ColumnDefinition.resultify(new Row(k, serializedCD)));
        assert cfm.equals(newCfm) : String.format("%n%s%n!=%n%s", cfm, newCfm);
    }
}
