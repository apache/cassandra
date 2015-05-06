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

import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.*;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class ThriftValidationTest extends SchemaLoader
{
    @Test(expected=org.apache.cassandra.exceptions.InvalidRequestException.class)
    public void testValidateCommutativeWithStandard() throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        ThriftValidation.validateColumnFamily("Keyspace1", "Standard1", true);
    }

    @Test
    public void testValidateCommutativeWithCounter() throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        ThriftValidation.validateColumnFamily("Keyspace1", "Counter1", true);
    }

    @Test
    public void testColumnNameEqualToKeyAlias() throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        CFMetaData metaData = Schema.instance.getCFMetaData("Keyspace1", "Standard1");
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
        ByteBuffer key = ByteBufferUtil.bytes("key");
        ThriftValidation.validateColumnData(newMetadata, key, null, column);
    }

    @Test
    public void testColumnNameEqualToDefaultKeyAlias() throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        CFMetaData metaData = Schema.instance.getCFMetaData("Keyspace1", "UUIDKeys");
        ColumnDefinition definition = metaData.getColumnDefinition(ByteBufferUtil.bytes(CFMetaData.DEFAULT_KEY_ALIAS));
        assertNotNull(definition);
        assertEquals(ColumnDefinition.Kind.PARTITION_KEY, definition.kind);

        // make sure the key alias does not affect validation of columns with the same name (CASSANDRA-6892)
        Column column = new Column(ByteBufferUtil.bytes(CFMetaData.DEFAULT_KEY_ALIAS));
        column.setValue(ByteBufferUtil.bytes("not a uuid"));
        column.setTimestamp(1234);
        ByteBuffer key = ByteBufferUtil.bytes("key");
        ThriftValidation.validateColumnData(metaData, key, null, column);

        IndexExpression expression = new IndexExpression(ByteBufferUtil.bytes(CFMetaData.DEFAULT_KEY_ALIAS), IndexOperator.EQ, ByteBufferUtil.bytes("a"));
        ThriftValidation.validateFilterClauses(metaData, Arrays.asList(expression));
    }

    @Test
    public void testColumnNameEqualToDefaultColumnAlias() throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        CFMetaData metaData = Schema.instance.getCFMetaData("Keyspace1", "StandardLong3");
        ColumnDefinition definition = metaData.getColumnDefinition(ByteBufferUtil.bytes(CFMetaData.DEFAULT_COLUMN_ALIAS + 1));
        assertNotNull(definition);

        // make sure the column alias does not affect validation of columns with the same name (CASSANDRA-6892)
        Column column = new Column(ByteBufferUtil.bytes(CFMetaData.DEFAULT_COLUMN_ALIAS + 1));
        column.setValue(ByteBufferUtil.bytes("not a long"));
        column.setTimestamp(1234);
        ByteBuffer key = ByteBufferUtil.bytes("key");
        ThriftValidation.validateColumnData(metaData, key, null, column);
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
            KSMetaData.fromThrift(ks_def).validate();
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
            KSMetaData.fromThrift(ks_def).validate();
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
            KSMetaData.fromThrift(ks_def).validate();
        }
        catch (ConfigurationException e)
        {
            gotException = true;
        }

        assert !gotException : "got unexpected ConfigurationException";
    }
}
