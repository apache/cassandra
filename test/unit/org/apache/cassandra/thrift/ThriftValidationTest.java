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

import org.junit.Test;

import java.util.Collections;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ThriftValidationTest extends SchemaLoader
{
    @Test(expected=InvalidRequestException.class)
    public void testValidateCommutativeWithStandard() throws InvalidRequestException
    {
        ThriftValidation.validateColumnFamily("Keyspace1", "Standard1", true);
    }

    @Test
    public void testValidateCommutativeWithCounter() throws InvalidRequestException
    {
        ThriftValidation.validateColumnFamily("Keyspace1", "Counter1", true);
    }

    @Test
    public void testColumnNameEqualToKeyAlias()
    {
        CFMetaData metaData = Schema.instance.getCFMetaData("Keyspace1", "Standard1");
        CFMetaData newMetadata = metaData.clone();

        boolean gotException = false;

        // add a key_alias = "id"
        newMetadata.keyAliases(Collections.singletonList(AsciiType.instance.decompose("id")));

        // should not throw IRE here
        try
        {
            newMetadata.validate();
        }
        catch (ConfigurationException e)
        {
            gotException = true;
        }

        assert !gotException : "got unexpected ConfigurationException";

        // add a column with name = "id"
        newMetadata.addColumnDefinition(new ColumnDefinition(ByteBufferUtil.bytes("id"), UTF8Type.instance, null, null, null, null));

        gotException = false;

        try
        {
            newMetadata.validate();
        }
        catch (ConfigurationException e)
        {
            gotException = true;
        }

        assert gotException : "expected ConfigurationException but not received.";
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
