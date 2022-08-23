/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.marshal;

import org.apache.cassandra.Util;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.UUID;

import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.AbstractTypeGenerators.primitiveTypeGen;
import static org.apache.cassandra.utils.AbstractTypeGenerators.userTypeGen;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUIDAsBytes;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.quicktheories.QuickTheory.qt;

public class TypeValidationTest
{
    @Test(expected = MarshalException.class)
    public void testInvalidAscii()
    {
        AsciiType.instance.validate(ByteBuffer.wrap(new byte[]{ (byte)0x80 }));
    }

    @Test(expected = MarshalException.class)
    public void testInvalidTimeUUID()
    {
        UUID uuid = UUID.randomUUID();
        TimeUUIDType.instance.validate(ByteBuffer.wrap(UUIDGen.decompose(uuid)));
    }

    @Test
    public void testValidTimeUUID()
    {
        TimeUUIDType.instance.validate(ByteBuffer.wrap(nextTimeUUIDAsBytes()));
    }

    @Test
    public void testLong()
    {
        LongType.instance.validate(Util.getBytes(5L));
        LongType.instance.validate(Util.getBytes(5555555555555555555L));
    }

    @Test
    public void testInt()
    {
        Int32Type.instance.validate(Util.getBytes(5));
        Int32Type.instance.validate(Util.getBytes(2057022603));
    }

    @Test
    public void testWriteValueWrongFixedLength()
    {
        DataOutputPlus output = Mockito.mock(DataOutputPlus.class);

        assertThatThrownBy(() -> Int32Type.instance.writeValue(Util.getBytes(42L), output))
        .isInstanceOf(IOException.class).hasMessageContaining("Expected exactly 4 bytes, but was 8");
        assertThatThrownBy(() -> LongType.instance.writeValue(Util.getBytes(42), output))
        .isInstanceOf(IOException.class).hasMessageContaining("Expected exactly 8 bytes, but was 4");
        assertThatThrownBy(() -> UUIDType.instance.writeValue(Util.getBytes(42L), output))
        .isInstanceOf(IOException.class).hasMessageContaining("Expected exactly 16 bytes, but was 8");

        Mockito.verifyNoInteractions(output);
    }

    @Test
    public void testValidUtf8() throws UnsupportedEncodingException
    {
        assert Character.MAX_CODE_POINT == 0x0010ffff;
        CharBuffer cb = CharBuffer.allocate(2837314);
        // let's test all of the unicode space.
        for (int i = 0; i < Character.MAX_CODE_POINT; i++)
        {
            // skip U+D800..U+DFFF. those CPs are invalid in utf8. java tolerates them, but doesn't convert them to
            // valid byte sequences (gives us '?' instead), so there is no point testing them.
            if (i >= 55296 && i <= 57343)
                continue;
            char[] ch = Character.toChars(i);
            for (char c : ch)
                cb.append(c);
        }
        String s = new String(cb.array());
        byte[] arr = s.getBytes("UTF8");
        ByteBuffer buf = ByteBuffer.wrap(arr);
        UTF8Type.instance.validate(buf);

        // some you might not expect.
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {}));
        // valid Utf8, unspecified in modified utf8.
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {0}));

        // modified utf8 null.
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {99, (byte)0xc0, (byte)0x80, 112}));

        // edges, for my sanity.
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {(byte)0xc2, (byte)0x81}));
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {(byte)0xe0, (byte)0xa0, (byte)0x81}));
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {(byte)0xf0, (byte)0x90, (byte)0x81, (byte)0x81}));
    }

    // now test for bogies.

    @Test(expected = MarshalException.class)
    public void testFloatingc0()
    {
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {99, (byte)0xc0, 112}));
    }

    @Test(expected = MarshalException.class)
    public void testInvalid2nd()
    {
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {(byte)0xc2, (byte)0xff}));
    }

    @Test(expected = MarshalException.class)
    public void testInvalid3rd()
    {
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {(byte)0xe0, (byte)0xa0, (byte)0xff}));
    }

    @Test(expected = MarshalException.class)
    public void testInvalid4th()
    {
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {(byte)0xf0, (byte)0x90, (byte)0x81, (byte)0xff}));
    }

    private static Gen<? extends TupleType> flatTupleGen()
    {
        return AbstractTypeGenerators.tupleTypeGen(primitiveTypeGen(), SourceDSL.integers().between(0, 20));
    }

    private static Gen<? extends TupleType> nestedTupleGen()
    {
        return AbstractTypeGenerators.tupleTypeGen();
    }

    private static Gen<? extends TupleType> flatUDTGen()
    {
        return userTypeGen(primitiveTypeGen(), SourceDSL.integers().between(0, 20));
    }

    private static  Gen<? extends TupleType> nestedUDTGen()
    {
        return AbstractTypeGenerators.userTypeGen();
    }

    @Test
    public void buildAndSplitTupleFlat()
    {
        buildAndSplit(flatTupleGen());
    }

    @Test
    public void buildAndSplitTupleNested()
    {
        buildAndSplit(nestedTupleGen());
    }

    @Test
    public void buildAndSplitUDTFlat()
    {
        buildAndSplit(flatUDTGen());
    }

    @Test
    public void buildAndSplitUDTNested()
    {
        buildAndSplit(nestedUDTGen());
    }

    private static void buildAndSplit(Gen<? extends TupleType> baseGen)
    {
        qt().forAll(tupleWithValueGen(baseGen)).checkAssert(pair -> {
            TupleType tuple = pair.left;
            ByteBuffer value = pair.right;
            Assertions.assertThat(TupleType.buildValue(tuple.split(ByteBufferAccessor.instance, value)))
                      .as("TupleType.buildValue(split(value)) == value")
                      .isEqualTo(value);
        });
    }

    @Test
    public void validateTupleFlat()
    {
        validate(flatTupleGen());
    }

    @Test
    public void validateTupleNested()
    {
        validate(nestedTupleGen());
    }

    private static void validate(Gen<? extends TupleType> baseGen)
    {
        qt().forAll(tupleWithValueGen(baseGen)).checkAssert(pair -> {
            TupleType tuple = pair.left;
            ByteBuffer value = pair.right;
            tuple.validate(value);
        });
    }

    private static Gen<Pair<TupleType, ByteBuffer>> tupleWithValueGen(Gen<? extends TupleType> baseGen)
    {
        Gen<Pair<TupleType, ByteBuffer>> gen = rnd -> {
            TupleType type = baseGen.generate(rnd);
            return Pair.create(type, getTypeSupport(type).valueGen.generate(rnd));
        };
        gen = gen.describedAs(pair -> pair.left.asCQL3Type().toString());
        return gen;
    }

    @Test
    public void validateUDTFlat()
    {
        validate(flatUDTGen());
    }

    @Test
    public void validateUDTNested()
    {
        validate(nestedUDTGen());
    }

    // todo: for completeness, should test invalid two byte pairs.
}
