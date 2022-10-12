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

package org.apache.cassandra.cql3.functions;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class UDAggregateTest extends CQLTester
{
    @Test
    public void testNewKeyspace()
    {
        String oldKeyspaceName = "old_ks";

        UserType userType = new UserType(oldKeyspaceName, ByteBufferUtil.bytes("a"),
                                         Arrays.asList(FieldIdentifier.forUnquoted("a1"),
                                                       FieldIdentifier.forUnquoted("a2"),
                                                       FieldIdentifier.forUnquoted("a3")),
                                         Arrays.asList(IntegerType.instance,
                                                       IntegerType.instance,
                                                       IntegerType.instance),
                                         true);

        String functionName = "my_func";
        UDFunction oldFunction = createUDFunction(oldKeyspaceName, functionName, userType);
        UDAggregate aggr = UDAggregate.create(Collections.singleton(oldFunction),
                                              new FunctionName(oldKeyspaceName, "my_aggregate"),
                                              Arrays.asList(userType, Int32Type.instance),
                                              Int32Type.instance,
                                              new FunctionName(oldKeyspaceName, functionName),
                                              null,
                                              Int32Type.instance,
                                              null);

        String newKeyspaceName = "new_ks";
        UDFunction newFunction = createUDFunction(newKeyspaceName, functionName, userType);
        UDAggregate newAggr = aggr.withNewKeyspace(newKeyspaceName, Collections.singletonList(newFunction), Types.of(userType));

        assertNotEquals(newAggr.name, aggr.name);
        assertEquals(newAggr.elementKeyspace(), newKeyspaceName);
    }

    private UDFunction createUDFunction(String oldKeyspaceName, String functionName, UserType userType)
    {
        return UDFunction.create(new FunctionName(oldKeyspaceName, functionName),
                                 Arrays.asList(ColumnIdentifier.getInterned("state", false),
                                               ColumnIdentifier.getInterned("val", false),
                                               ColumnIdentifier.getInterned("val2", false)),
                                 Arrays.asList(Int32Type.instance, userType, Int32Type.instance),
                                 Int32Type.instance,
                                 true,
                                 "java",
                                 "return val2;");
    }
}
