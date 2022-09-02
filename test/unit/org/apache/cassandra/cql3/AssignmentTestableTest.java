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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;

public class AssignmentTestableTest
{
    @Test
    public void testGetPreferredCompatibleType()
    {
        testGetPreferredCompatibleType(null);

        testGetPreferredCompatibleType(AsciiType.instance, AsciiType.instance);
        testGetPreferredCompatibleType(UTF8Type.instance, UTF8Type.instance);
        testGetPreferredCompatibleType(AsciiType.instance, AsciiType.instance, AsciiType.instance);
        testGetPreferredCompatibleType(UTF8Type.instance, UTF8Type.instance, UTF8Type.instance);
        testGetPreferredCompatibleType(UTF8Type.instance, AsciiType.instance, UTF8Type.instance);
        testGetPreferredCompatibleType(UTF8Type.instance, UTF8Type.instance, AsciiType.instance);

        testGetPreferredCompatibleType(Int32Type.instance, Int32Type.instance);
        testGetPreferredCompatibleType(LongType.instance, LongType.instance);
        testGetPreferredCompatibleType(Int32Type.instance, Int32Type.instance, Int32Type.instance);
        testGetPreferredCompatibleType(LongType.instance, LongType.instance, LongType.instance);
        testGetPreferredCompatibleType(null, Int32Type.instance, LongType.instance); // not assignable
        testGetPreferredCompatibleType(null, LongType.instance, Int32Type.instance); // not assignable
    }

    public void testGetPreferredCompatibleType(AbstractType<?> type, AbstractType<?>... types)
    {
        Assert.assertEquals(type, AssignmentTestable.getCompatibleTypeIfKnown(Arrays.asList(types)));
    }
}
