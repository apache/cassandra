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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnDefinitionTest
{
    @Test
    public void testSerializeDeserialize() throws Exception
    {
        CFMetaData cfm = CFMetaData.denseCFMetaData("ks", "cf", UTF8Type.instance);

        ColumnDefinition cd0 = ColumnDefinition.regularDef(cfm, ByteBufferUtil.bytes("TestColumnDefinitionName0"), BytesType.instance, null)
                                               .setIndex("random index name 0", IndexType.KEYS, null);

        ColumnDefinition cd1 = ColumnDefinition.regularDef(cfm, ByteBufferUtil.bytes("TestColumnDefinition1"), LongType.instance, null);

        testSerializeDeserialize(cfm, cd0);
        testSerializeDeserialize(cfm, cd1);
    }

    protected void testSerializeDeserialize(CFMetaData cfm, ColumnDefinition cd) throws Exception
    {
        ColumnDefinition newCd = ColumnDefinition.fromThrift(cfm.ksName, cfm.cfName, cfm.comparator.asAbstractType(), null, cd.toThrift());
        Assert.assertNotSame(cd, newCd);
        Assert.assertEquals(cd.hashCode(), newCd.hashCode());
        Assert.assertEquals(cd, newCd);
    }
}
