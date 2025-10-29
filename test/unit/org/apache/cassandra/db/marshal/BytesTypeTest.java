/**
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
package org.apache.cassandra.db.marshal;

import java.util.Arrays;

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BytesTypeTest
{
    private static final String INVALID_HEX = "33AG45F"; // Invalid (has a G)
    private static final String VALID_HEX = "33A45F";

    @Test (expected = MarshalException.class)
    public void testFromStringWithInvalidString()
    {
        BytesType.instance.fromString(INVALID_HEX);
    }

    @Test
    public void testFromStringWithValidString()
    {
        BytesType.instance.fromString(VALID_HEX);
    }

    @Test
    public void testValueCompatibilityWithScalarTypes()
    {
        // BytesType should be value-compatible with simple scalar types
        assertTrue("BytesType should be value-compatible with AsciiType",
                   BytesType.instance.isValueCompatibleWith(AsciiType.instance));
        assertTrue("BytesType should be value-compatible with UTF8Type",
                   BytesType.instance.isValueCompatibleWith(UTF8Type.instance));
        assertTrue("BytesType should be value-compatible with Int32Type",
                   BytesType.instance.isValueCompatibleWith(Int32Type.instance));
        assertTrue("BytesType should be value-compatible with LongType",
                   BytesType.instance.isValueCompatibleWith(LongType.instance));
        assertTrue("BytesType should be value-compatible with UUIDType",
                   BytesType.instance.isValueCompatibleWith(UUIDType.instance));
        assertTrue("BytesType should be value-compatible with BooleanType",
                   BytesType.instance.isValueCompatibleWith(BooleanType.instance));
        assertTrue("BytesType should be value-compatible with itself",
                   BytesType.instance.isValueCompatibleWith(BytesType.instance));
    }

    @Test
    public void testSerializationIncompatibilityWithCollections()
    {
        // BytesType should NOT be serialization-compatible with collections (even frozen ones)
        // because converting a collection to raw bytes for schema changes is nonsensical
        ListType<?> frozenList = ListType.getInstance(Int32Type.instance, false);
        SetType<?> frozenSet = SetType.getInstance(Int32Type.instance, false);
        MapType<?, ?> frozenMap = MapType.getInstance(Int32Type.instance, UTF8Type.instance, false);

        assertFalse("BytesType should NOT be serialization-compatible with frozen<list>",
                    BytesType.instance.isSerializationCompatibleWith(frozenList));
        assertFalse("BytesType should NOT be serialization-compatible with frozen<set>",
                    BytesType.instance.isSerializationCompatibleWith(frozenSet));
        assertFalse("BytesType should NOT be serialization-compatible with frozen<map>",
                    BytesType.instance.isSerializationCompatibleWith(frozenMap));

        // Also test with multi-cell collections
        ListType<?> multiCellList = ListType.getInstance(Int32Type.instance, true);
        SetType<?> multiCellSet = SetType.getInstance(Int32Type.instance, true);
        MapType<?, ?> multiCellMap = MapType.getInstance(Int32Type.instance, UTF8Type.instance, true);

        assertFalse("BytesType should NOT be serialization-compatible with list",
                    BytesType.instance.isSerializationCompatibleWith(multiCellList));
        assertFalse("BytesType should NOT be serialization-compatible with set",
                    BytesType.instance.isSerializationCompatibleWith(multiCellSet));
        assertFalse("BytesType should NOT be serialization-compatible with map",
                    BytesType.instance.isSerializationCompatibleWith(multiCellMap));
    }

    @Test
    public void testSerializationIncompatibilityWithUDT()
    {
        // BytesType should NOT be serialization-compatible with User Defined Types
        // because converting a UDT to raw bytes for schema changes is nonsensical
        UserType udt = new UserType("ks",
                                     ByteBufferUtil.bytes("myType"),
                                     Arrays.asList(FieldIdentifier.forQuoted("field1"), FieldIdentifier.forQuoted("field2")),
                                     Arrays.asList(Int32Type.instance, UTF8Type.instance),
                                     true);

        assertFalse("BytesType should NOT be serialization-compatible with UDT",
                    BytesType.instance.isSerializationCompatibleWith(udt));
    }
}
