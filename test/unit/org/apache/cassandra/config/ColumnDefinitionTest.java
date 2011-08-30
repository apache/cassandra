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


import org.junit.Test;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnDefinitionTest
{
    @Test
    public void testSerializeDeserialize() throws Exception
    {
        ColumnDefinition cd0 = new ColumnDefinition(ByteBufferUtil.bytes("TestColumnDefinitionName0"),
                                                    BytesType.instance,
                                                    IndexType.KEYS,
                                                    null,
                                                    "random index name 0");

        ColumnDefinition cd1 = new ColumnDefinition(ByteBufferUtil.bytes("TestColumnDefinition1"),
                                                    LongType.instance,
                                                    null,
                                                    null,
                                                    null);

        testSerializeDeserialize(cd0);
        testSerializeDeserialize(cd1);
    }

    protected void testSerializeDeserialize(ColumnDefinition cd) throws Exception
    {
        ColumnDefinition newCd = ColumnDefinition.fromAvro(cd.toAvro());
        assert cd != newCd;
        assert cd.hashCode() == newCd.hashCode();
        assert cd.equals(newCd);
    }
}
