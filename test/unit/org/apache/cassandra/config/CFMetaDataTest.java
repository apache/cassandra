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

import org.apache.avro.util.Utf8;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CFMetaDataTest
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
    public void testThriftToAvroConversion() throws Exception
    {
        CfDef cfDef = new CfDef().setDefault_validation_class(AsciiType.class.getCanonicalName())
                                 .setComment("Test comment")
                                 .setColumn_metadata(columnDefs)
                                 .setKeyspace(KEYSPACE)
                                 .setName(COLUMN_FAMILY);

        // convert Thrift to CFMetaData
        CFMetaData cfMetaData = CFMetaData.fromThrift(cfDef);

        // make a correct Avro object
        org.apache.cassandra.db.migration.avro.CfDef avroCfDef = new org.apache.cassandra.db.migration.avro.CfDef();
        avroCfDef.keyspace = new Utf8(KEYSPACE);
        avroCfDef.name = new Utf8(COLUMN_FAMILY);
        avroCfDef.default_validation_class = new Utf8(cfDef.default_validation_class);
        avroCfDef.comment = new Utf8(cfDef.comment);
        avroCfDef.column_metadata = new ArrayList<org.apache.cassandra.db.migration.avro.ColumnDef>();
        for (ColumnDef columnDef : columnDefs)
        {
            org.apache.cassandra.db.migration.avro.ColumnDef c = new org.apache.cassandra.db.migration.avro.ColumnDef();
            c.name = ByteBufferUtil.clone(columnDef.name);
            c.validation_class = new Utf8(columnDef.getValidation_class());
            c.index_name = new Utf8(columnDef.getIndex_name());
            c.index_type = org.apache.cassandra.db.migration.avro.IndexType.KEYS;
            avroCfDef.column_metadata.add(c);
        }

        org.apache.cassandra.db.migration.avro.CfDef converted = cfMetaData.toAvro();

        assertEquals(avroCfDef.keyspace, converted.keyspace);
        assertEquals(avroCfDef.name, converted.name);
        assertEquals(avroCfDef.default_validation_class, converted.default_validation_class);
        assertEquals(avroCfDef.comment, converted.comment);
        assertEquals(avroCfDef.column_metadata, converted.column_metadata);
    }
}
