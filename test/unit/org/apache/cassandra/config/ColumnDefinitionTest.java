package org.apache.cassandra.config;
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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnDefinitionTest
{
    @Test
    public void testSerializeDeserialize() throws Exception
    {
        CFMetaData cfm = CFMetaData.Builder.create("ks", "cf", true, false, false)
                         .addPartitionKey("pkey", AsciiType.instance)
                         .addClusteringColumn("name", AsciiType.instance)
                         .addRegularColumn("val", AsciiType.instance)
                         .build();

        ColumnDefinition cd0 = ColumnDefinition.staticDef(cfm, ByteBufferUtil.bytes("TestColumnDefinitionName0"), BytesType.instance);
        ColumnDefinition cd1 = ColumnDefinition.staticDef(cfm, ByteBufferUtil.bytes("TestColumnDefinition1"), LongType.instance);

        testSerializeDeserialize(cfm, cd0);
        testSerializeDeserialize(cfm, cd1);
    }

    protected void testSerializeDeserialize(CFMetaData cfm, ColumnDefinition cd) throws Exception
    {
        ColumnDefinition newCd = ThriftConversion.fromThrift(cfm.ksName, cfm.cfName, cfm.comparator.subtype(0), null, ThriftConversion.toThrift(cfm, cd));
        Assert.assertNotSame(cd, newCd);
        Assert.assertEquals(cd.hashCode(), newCd.hashCode());
        Assert.assertEquals(cd, newCd);
    }
}
