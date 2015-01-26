package org.apache.cassandra.thrift;
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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class ThriftValidationTest
{
    public static final String KEYSPACE1 = "MultiSliceTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_COUNTER = "Counter1";
    public static final String CF_UUID = "UUIDKeys";
    public static final String CF_STANDARDLONG3 = "StandardLong3";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD),
                                    CFMetaData.denseCFMetaData(KEYSPACE1, CF_COUNTER, BytesType.instance).defaultValidator(CounterColumnType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_UUID).keyValidator(UUIDType.instance),
                                    CFMetaData.denseCFMetaData(KEYSPACE1, CF_STANDARDLONG3, IntegerType.instance));
    }
    
    @Test(expected=org.apache.cassandra.exceptions.InvalidRequestException.class)
    public void testValidateCommutativeWithStandard() throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        ThriftValidation.validateColumnFamily(KEYSPACE1, "Standard1", true);
    }

    @Test
    public void testValidateCommutativeWithCounter() throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        ThriftValidation.validateColumnFamily(KEYSPACE1, "Counter1", true);
    }

    @Test
    public void testColumnNameEqualToKeyAlias() throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        CFMetaData metaData = Schema.instance.getCFMetaData(KEYSPACE1, "Standard1");
        CFMetaData newMetadata = metaData.copy();

        boolean gotException = false;

        // add a key_alias = "id"
        // should not throw IRE here
        try
        {
            newMetadata.addColumnDefinition(ColumnDefinition.partitionKeyDef(metaData, AsciiType.instance.decompose("id"), LongType.instance, null));
            newMetadata.validate();
        }
        catch (ConfigurationException e)
        {
            gotException = true;
        }

        assert !gotException : "got unexpected ConfigurationException";


        gotException = false;

        // add a column with name = "id"
        try
        {
            newMetadata.addColumnDefinition(ColumnDefinition.regularDef(metaData, ByteBufferUtil.bytes("id"), LongType.instance, null));
            newMetadata.validate();
        }
        catch (ConfigurationException e)
        {
            gotException = true;
        }

        assert gotException : "expected ConfigurationException but not received.";

        // make sure the key alias does not affect validation of columns with the same name (CASSANDRA-6892)
        Column column = new Column(ByteBufferUtil.bytes("id"));
        column.setValue(ByteBufferUtil.bytes("not a long"));
        column.setTimestamp(1234);
        ThriftValidation.validateColumnData(newMetadata, null, column);
    }

    @Test
    public void testColumnNameEqualToDefaultKeyAlias() throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        CFMetaData metaData = Schema.instance.getCFMetaData(KEYSPACE1, "UUIDKeys");
        ColumnDefinition definition = metaData.getColumnDefinition(ByteBufferUtil.bytes(CFMetaData.DEFAULT_KEY_ALIAS));
        assertNotNull(definition);
        assertEquals(ColumnDefinition.Kind.PARTITION_KEY, definition.kind);

        // make sure the key alias does not affect validation of columns with the same name (CASSANDRA-6892)
        Column column = new Column(ByteBufferUtil.bytes(CFMetaData.DEFAULT_KEY_ALIAS));
        column.setValue(ByteBufferUtil.bytes("not a uuid"));
        column.setTimestamp(1234);
        ThriftValidation.validateColumnData(metaData, null, column);

        IndexExpression expression = new IndexExpression(ByteBufferUtil.bytes(CFMetaData.DEFAULT_KEY_ALIAS), IndexOperator.EQ, ByteBufferUtil.bytes("a"));
        ThriftValidation.validateFilterClauses(metaData, Arrays.asList(expression));
    }

    @Test
    public void testColumnNameEqualToDefaultColumnAlias() throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        CFMetaData metaData = Schema.instance.getCFMetaData(KEYSPACE1, "StandardLong3");
        ColumnDefinition definition = metaData.getColumnDefinition(ByteBufferUtil.bytes(CFMetaData.DEFAULT_COLUMN_ALIAS + 1));
        assertNotNull(definition);

        // make sure the column alias does not affect validation of columns with the same name (CASSANDRA-6892)
        Column column = new Column(ByteBufferUtil.bytes(CFMetaData.DEFAULT_COLUMN_ALIAS + 1));
        column.setValue(ByteBufferUtil.bytes("not a long"));
        column.setTimestamp(1234);
        ThriftValidation.validateColumnData(metaData, null, column);
    }

    @Test
    public void testValidateKsDef()
    {
        KsDef ks_def = new KsDef()
                            .setName("keyspaceValid")
                            .setStrategy_class(LocalStrategy.class.getSimpleName());


        boolean gotException = false;

        try
        {
            ThriftConversion.fromThrift(ks_def).validate();
        }
        catch (ConfigurationException e)
        {
            gotException = true;
        }

        assert gotException : "expected ConfigurationException but not received.";

        ks_def.setStrategy_class(LocalStrategy.class.getName());

        gotException = false;

        try
        {
            ThriftConversion.fromThrift(ks_def).validate();
        }
        catch (ConfigurationException e)
        {
            gotException = true;
        }

        assert gotException : "expected ConfigurationException but not received.";

        ks_def.setStrategy_class(NetworkTopologyStrategy.class.getName());

        gotException = false;

        try
        {
            ThriftConversion.fromThrift(ks_def).validate();
        }
        catch (ConfigurationException e)
        {
            gotException = true;
        }

        assert !gotException : "got unexpected ConfigurationException";
    }
}
