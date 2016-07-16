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
package org.apache.cassandra.cql3;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class ThriftCompatibilityTest extends SchemaLoader
{
    @Test // test for CASSANDRA-8178
    public void testNonTextComparator() throws Throwable
    {
        ColumnDef column = new ColumnDef();
        column.setName(bytes(42))
              .setValidation_class(UTF8Type.instance.toString());

        CfDef cf = new CfDef("thriftcompat", "JdbcInteger");
        cf.setColumn_type("Standard")
          .setComparator_type(Int32Type.instance.toString())
          .setDefault_validation_class(UTF8Type.instance.toString())
          .setKey_validation_class(BytesType.instance.toString())
          .setColumn_metadata(Collections.singletonList(column));

        SchemaLoader.createKeyspace("thriftcompat", KeyspaceParams.simple(1), ThriftConversion.fromThrift(cf));

        // the comparator is IntegerType, and there is a column named 42 with a UTF8Type validation type
        execute("INSERT INTO \"thriftcompat\".\"JdbcInteger\" (key, \"42\") VALUES (0x00000001, 'abc')");
        execute("UPDATE \"thriftcompat\".\"JdbcInteger\" SET \"42\" = 'abc' WHERE key = 0x00000001");
        execute("DELETE \"42\" FROM \"thriftcompat\".\"JdbcInteger\" WHERE key = 0x00000000");
        UntypedResultSet results = execute("SELECT key, \"42\" FROM \"thriftcompat\".\"JdbcInteger\"");
        assertEquals(1, results.size());
        UntypedResultSet.Row row = results.iterator().next();
        assertEquals(ByteBufferUtil.bytes(1), row.getBytes("key"));
        assertEquals("abc", row.getString("42"));
    }

    @Test // test for CASSANDRA-9867
    public void testDropCompactStaticColumn()
    {
        ColumnDef column1 = new ColumnDef();
        column1.setName(bytes(42))
              .setValidation_class(UTF8Type.instance.toString());

        ColumnDef column2 = new ColumnDef();
        column2.setName(bytes(25))
               .setValidation_class(UTF8Type.instance.toString());

        CfDef cf = new CfDef("thriftks", "staticcompact");
        cf.setColumn_type("Standard")
          .setComparator_type(Int32Type.instance.toString())
          .setDefault_validation_class(UTF8Type.instance.toString())
          .setKey_validation_class(BytesType.instance.toString())
          .setColumn_metadata(Arrays.asList(column1, column2));

        SchemaLoader.createKeyspace("thriftks", KeyspaceParams.simple(1), ThriftConversion.fromThrift(cf));
        CFMetaData cfm = Schema.instance.getCFMetaData("thriftks", "staticcompact");

        // assert the both columns are in the metadata
        assertTrue(cfm.getColumnMetadata().containsKey(bytes(42)));
        assertTrue(cfm.getColumnMetadata().containsKey(bytes(25)));

        // remove column2
        cf.setColumn_metadata(Collections.singletonList(column1));
        MigrationManager.announceColumnFamilyUpdate(ThriftConversion.fromThriftForUpdate(cf, cfm), true);

        // assert that it's gone from metadata
        assertTrue(cfm.getColumnMetadata().containsKey(bytes(42)));
        assertFalse(cfm.getColumnMetadata().containsKey(bytes(25)));
    }

    private static UntypedResultSet execute(String query)
    {
        return QueryProcessor.executeInternal(query);
    }
}
