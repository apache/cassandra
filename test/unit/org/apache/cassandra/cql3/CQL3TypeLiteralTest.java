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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.serializers.*;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;

/**
 * Test functionality to re-create a CQL literal from its serialized representation.
 * This test uses some randomness to generate the values and nested structures (collections,tuples,UDTs).
 */
public class CQL3TypeLiteralTest
{
    private static final Pattern QUOTE = Pattern.compile("'");

    /**
     * Container holding the expected CQL literal for a type and serialized value.
     * The CQL literal is generated independently from the code in {@link CQL3Type}.
     */
    static class Value
    {
        final String expected;
        final CQL3Type cql3Type;
        final ByteBuffer value;

        Value(String expected, CQL3Type cql3Type, ByteBuffer value)
        {
            this.expected = expected;
            this.cql3Type = cql3Type;
            this.value = value;
        }
    }

    static final Map<CQL3Type.Native, List<Value>> nativeTypeValues = new EnumMap<>(CQL3Type.Native.class);

    static void addNativeValue(String expected, CQL3Type.Native cql3Type, ByteBuffer value)
    {
        List<Value> l = nativeTypeValues.get(cql3Type);
        if (l == null)
            nativeTypeValues.put(cql3Type, l = new ArrayList<>());
        l.add(new Value(expected, cql3Type, value));
    }

    static
    {
        // Add some (random) values for each native type.
        // Also adds null values and empty values, if the type allows this.

        for (int i = 0; i < 20; i++)
        {
            String v = randString(true);
            addNativeValue(quote(v), CQL3Type.Native.ASCII, AsciiSerializer.instance.serialize(v));
        }
        addNativeValue("''", CQL3Type.Native.ASCII, AsciiSerializer.instance.serialize(""));
        addNativeValue("''", CQL3Type.Native.ASCII, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.ASCII, null);

        for (int i = 0; i < 20; i++)
        {
            String v = randString(false);
            addNativeValue(quote(v), CQL3Type.Native.TEXT, UTF8Serializer.instance.serialize(v));
        }
        addNativeValue("''", CQL3Type.Native.TEXT, UTF8Serializer.instance.serialize(""));
        addNativeValue("''", CQL3Type.Native.TEXT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.TEXT, null);

        for (int i = 0; i < 20; i++)
        {
            String v = randString(false);
            addNativeValue(quote(v), CQL3Type.Native.VARCHAR, UTF8Serializer.instance.serialize(v));
        }
        addNativeValue("''", CQL3Type.Native.VARCHAR, UTF8Serializer.instance.serialize(""));
        addNativeValue("''", CQL3Type.Native.VARCHAR, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.VARCHAR, null);

        addNativeValue("0", CQL3Type.Native.BIGINT, LongType.instance.decompose(0L));
        for (int i = 0; i < 20; i++)
        {
            long v = randLong();
            addNativeValue(Long.toString(v), CQL3Type.Native.BIGINT, LongType.instance.decompose(v));
        }
        addNativeValue("null", CQL3Type.Native.BIGINT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.BIGINT, null);

        addNativeValue("0", CQL3Type.Native.COUNTER, LongType.instance.decompose(0L));
        for (int i = 0; i < 20; i++)
        {
            long v = randLong();
            addNativeValue(Long.toString(v), CQL3Type.Native.COUNTER, LongType.instance.decompose(v));
        }
        addNativeValue("null", CQL3Type.Native.COUNTER, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.COUNTER, null);

        addNativeValue("0", CQL3Type.Native.INT, Int32Type.instance.decompose(0));
        for (int i = 0; i < 20; i++)
        {
            int v = randInt();
            addNativeValue(Integer.toString(v), CQL3Type.Native.INT, Int32Type.instance.decompose(v));
        }
        addNativeValue("null", CQL3Type.Native.INT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.INT, null);

        addNativeValue("0", CQL3Type.Native.SMALLINT, ShortType.instance.decompose((short) 0));
        for (int i = 0; i < 20; i++)
        {
            short v = randShort();
            addNativeValue(Short.toString(v), CQL3Type.Native.SMALLINT, ShortType.instance.decompose(v));
        }
        addNativeValue("null", CQL3Type.Native.SMALLINT, null);

        addNativeValue("0", CQL3Type.Native.TINYINT, ByteType.instance.decompose((byte) 0));
        for (int i = 0; i < 20; i++)
        {
            byte v = randByte();
            addNativeValue(Short.toString(v), CQL3Type.Native.TINYINT, ByteType.instance.decompose(v));
        }
        addNativeValue("null", CQL3Type.Native.TINYINT, null);

        addNativeValue("0.0", CQL3Type.Native.FLOAT, FloatType.instance.decompose((float) 0));
        for (int i = 0; i < 20; i++)
        {
            float v = randFloat();
            addNativeValue(Float.toString(v), CQL3Type.Native.FLOAT, FloatType.instance.decompose(v));
        }
        addNativeValue("NaN", CQL3Type.Native.FLOAT, FloatType.instance.decompose(Float.NaN));
        addNativeValue("null", CQL3Type.Native.FLOAT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.FLOAT, null);

        addNativeValue("0.0", CQL3Type.Native.DOUBLE, DoubleType.instance.decompose((double) 0));
        for (int i = 0; i < 20; i++)
        {
            double v = randDouble();
            addNativeValue(Double.toString(v), CQL3Type.Native.DOUBLE, DoubleType.instance.decompose(v));
        }
        addNativeValue("NaN", CQL3Type.Native.DOUBLE, DoubleType.instance.decompose(Double.NaN));
        addNativeValue("null", CQL3Type.Native.DOUBLE, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.DOUBLE, null);

        addNativeValue("0", CQL3Type.Native.DECIMAL, DecimalType.instance.decompose(BigDecimal.ZERO));
        for (int i = 0; i < 20; i++)
        {
            BigDecimal v = BigDecimal.valueOf(randDouble());
            addNativeValue(v.toString(), CQL3Type.Native.DECIMAL, DecimalType.instance.decompose(v));
        }
        addNativeValue("null", CQL3Type.Native.DECIMAL, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.DECIMAL, null);

        addNativeValue("0", CQL3Type.Native.VARINT, IntegerType.instance.decompose(BigInteger.ZERO));
        for (int i = 0; i < 20; i++)
        {
            BigInteger v = BigInteger.valueOf(randLong());
            addNativeValue(v.toString(), CQL3Type.Native.VARINT, IntegerType.instance.decompose(v));
        }
        addNativeValue("null", CQL3Type.Native.VARINT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.VARINT, null);

        // boolean doesn't have that many possible values...
        addNativeValue("false", CQL3Type.Native.BOOLEAN, BooleanType.instance.decompose(false));
        addNativeValue("true", CQL3Type.Native.BOOLEAN, BooleanType.instance.decompose(true));
        addNativeValue("null", CQL3Type.Native.BOOLEAN, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.BOOLEAN, null);

        // (mostly generates date values with surreal values like in year 14273)
        for (int i = 0; i < 20; i++)
        {
            int v = randInt();
            addNativeValue(SimpleDateSerializer.instance.toString(v), CQL3Type.Native.DATE, SimpleDateSerializer.instance.serialize(v));
        }
        addNativeValue("null", CQL3Type.Native.DATE, null);

        for (int i = 0; i < 100; i++)
        {
            long v = randLong(24L * 60 * 60 * 1000 * 1000 * 1000);
            addNativeValue(TimeSerializer.instance.toString(v), CQL3Type.Native.TIME, TimeSerializer.instance.serialize(v));
        }
        addNativeValue("null", CQL3Type.Native.TIME, null);

        // (mostly generates timestamp values with surreal values like in year 14273)
        for (int i = 0; i < 20; i++)
        {
            long v = randLong();
            addNativeValue(TimestampSerializer.instance.toStringUTC(new Date(v)), CQL3Type.Native.TIMESTAMP, TimestampType.instance.fromString(Long.toString(v)));
        }
        addNativeValue("null", CQL3Type.Native.TIMESTAMP, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.TIMESTAMP, null);

        for (int i = 0; i < 20; i++)
        {
            UUID v = UUIDGen.getTimeUUID(randLong(System.currentTimeMillis()));
            addNativeValue(v.toString(), CQL3Type.Native.TIMEUUID, TimeUUIDType.instance.decompose(v));
        }
        addNativeValue("null", CQL3Type.Native.TIMEUUID, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.TIMEUUID, null);

        for (int i = 0; i < 20; i++)
        {
            UUID v = UUID.randomUUID();
            addNativeValue(v.toString(), CQL3Type.Native.UUID, UUIDType.instance.decompose(v));
        }
        addNativeValue("null", CQL3Type.Native.UUID, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.UUID, null);

        for (int i = 0; i < 20; i++)
        {
            ByteBuffer v = randBytes();
            addNativeValue("0x" + BytesSerializer.instance.toString(v), CQL3Type.Native.BLOB, BytesType.instance.decompose(v));
        }
        addNativeValue("0x", CQL3Type.Native.BLOB, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.BLOB, null);

        for (int i = 0; i < 20; i++)
        {
            InetAddress v;
            try
            {
                v = InetAddress.getByAddress(new byte[]{ randByte(), randByte(), randByte(), randByte() });
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
            addNativeValue(v.getHostAddress(), CQL3Type.Native.INET, InetAddressSerializer.instance.serialize(v));
        }
        addNativeValue("null", CQL3Type.Native.INET, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        addNativeValue("null", CQL3Type.Native.INET, null);
    }

    @Test
    public void testNative()
    {
        // test each native type against each supported protocol version (although it doesn't make sense to
        // iterate through all protocol versions as of C* 3.0).

        for (int version = Server.MIN_SUPPORTED_VERSION; version <= Server.CURRENT_VERSION; version++)
        {
            for (Map.Entry<CQL3Type.Native, List<Value>> entry : nativeTypeValues.entrySet())
            {
                for (Value value : entry.getValue())
                {
                    compareCqlLiteral(version, value);
                }
            }
        }
    }

    @Test
    public void testCollectionWithNatives()
    {
        // test 100 collections with varying element/key/value types against each supported protocol version,
        // type of collection is randomly chosen

        for (int version = Server.MIN_SUPPORTED_VERSION; version <= Server.CURRENT_VERSION; version++)
        {
            for (int n = 0; n < 100; n++)
            {
                Value value = generateCollectionValue(version, randomCollectionType(0), true);
                compareCqlLiteral(version, value);
            }
        }
    }

    @Test
    public void testCollectionNullAndEmpty()
    {
        // An empty collection is one with a size of 0 (note that rely on the fact that protocol version < 3 are not
        // supported anymore and so the size of a collection is always on 4 bytes).
        ByteBuffer emptyCollection = ByteBufferUtil.bytes(0);

        for (int version = Server.MIN_SUPPORTED_VERSION; version <= Server.CURRENT_VERSION; version++)
        {
            for (boolean frozen : Arrays.asList(true, false))
            {
                // empty
                Value value = new Value("[]", ListType.getInstance(UTF8Type.instance, frozen).asCQL3Type(), emptyCollection);
                compareCqlLiteral(version, value);
                value = new Value("{}", SetType.getInstance(UTF8Type.instance, frozen).asCQL3Type(), emptyCollection);
                compareCqlLiteral(version, value);
                value = new Value("{}", MapType.getInstance(UTF8Type.instance, UTF8Type.instance, frozen).asCQL3Type(), emptyCollection);
                compareCqlLiteral(version, value);

                // null
                value = new Value("null", ListType.getInstance(UTF8Type.instance, frozen).asCQL3Type(), null);
                compareCqlLiteral(version, value);
                value = new Value("null", SetType.getInstance(UTF8Type.instance, frozen).asCQL3Type(), null);
                compareCqlLiteral(version, value);
                value = new Value("null", MapType.getInstance(UTF8Type.instance, UTF8Type.instance, frozen).asCQL3Type(), null);
                compareCqlLiteral(version, value);
            }
        }
    }

    @Test
    public void testTupleWithNatives()
    {
        // test 100 tuples with varying element/key/value types against each supported protocol version

        for (int version = Server.MIN_SUPPORTED_VERSION; version <= Server.CURRENT_VERSION; version++)
        {
            for (int n = 0; n < 100; n++)
            {
                Value value = generateTupleValue(version, randomTupleType(0), true);
                compareCqlLiteral(version, value);
            }
        }
    }

    @Test
    public void testUserDefinedWithNatives()
    {
        // test 100 UDTs with varying element/key/value types against each supported protocol version

        for (int version = Server.MIN_SUPPORTED_VERSION; version <= Server.CURRENT_VERSION; version++)
        {
            for (int n = 0; n < 100; n++)
            {
                Value value = generateUserDefinedValue(version, randomUserType(0), true);
                compareCqlLiteral(version, value);
            }
        }
    }

    @Test
    public void testNested()
    {
        // This is the "nice" part of this unit test - it tests (probably) nested type structures
        // like 'tuple<map, list<user>, tuple, user>' or 'map<tuple<int, text>, set<inet>>' with
        // random types  against each supported protocol version.

        for (int version = Server.MIN_SUPPORTED_VERSION; version <= Server.CURRENT_VERSION; version++)
        {
            for (int n = 0; n < 100; n++)
            {
                Value value = randomNested(version);
                compareCqlLiteral(version, value);
            }
        }
    }

    static void compareCqlLiteral(int version, Value value)
    {
        ByteBuffer buffer = value.value != null ? value.value.duplicate() : null;
        String msg = "Failed to get expected value for type " + value.cql3Type + " / " + value.cql3Type.getType() + " with protocol-version " + version + " expected:\"" + value.expected + '"';
        try
        {
            assertEquals(msg,
                         value.expected,
                         value.cql3Type.toCQLLiteral(buffer, version));
        }
        catch (RuntimeException e)
        {
            throw new RuntimeException(msg, e);
        }
    }

    static Value randomNested(int version)
    {
        AbstractType type = randomNestedType(2);

        return generateAnyValue(version, type.asCQL3Type());
    }

    /**
     * Generates type of randomly nested type structures.
     */
    static AbstractType randomNestedType(int level)
    {
        if (level == 0)
            return randomNativeType();
        switch (randInt(level == 2 ? 3 : 4))
        {
            case 0:
                return randomCollectionType(level - 1);
            case 1:
                return randomTupleType(level - 1);
            case 2:
                return randomUserType(level - 1);
            case 3:
                return randomNativeType();
        }
        throw new AssertionError();
    }

    static Value generateCollectionValue(int version, CollectionType collectionType, boolean allowNull)
    {
        StringBuilder expected = new StringBuilder();
        ByteBuffer buffer;

        if (allowNull && randBool(0.05d))
        {
            expected.append("null");
            buffer = null;
        }
        else
        {
            int size = randInt(20);

            CQL3Type elements;
            CQL3Type values = null;
            char bracketOpen;
            char bracketClose;
            switch (collectionType.kind)
            {
                case LIST:
                    elements = ((ListType) collectionType).getElementsType().asCQL3Type();
                    bracketOpen = '[';
                    bracketClose = ']';
                    break;
                case SET:
                    elements = ((SetType) collectionType).getElementsType().asCQL3Type();
                    bracketOpen = '{';
                    bracketClose = '}';
                    break;
                case MAP:
                    elements = ((MapType) collectionType).getKeysType().asCQL3Type();
                    values = ((MapType) collectionType).getValuesType().asCQL3Type();
                    bracketOpen = '{';
                    bracketClose = '}';
                    break;
                default:
                    throw new AssertionError();
            }

            expected.append(bracketOpen);
            Collection<ByteBuffer> buffers = new ArrayList<>();
            Set<ByteBuffer> added = new HashSet<>();
            for (int i = 0; i < size; i++)
            {
                Value el = generateAnyValue(version, elements);
                if (!added.add(el.value))
                    continue;

                buffers.add(el.value.duplicate());
                if (expected.length() > 1)
                    expected.append(", ");
                expected.append(el.cql3Type.toCQLLiteral(el.value, version));

                if (collectionType.kind == CollectionType.Kind.MAP)
                {
                    // add map value
                    el = generateAnyValue(version, values);
                    buffers.add(el.value.duplicate());
                    expected.append(": ");
                    expected.append(el.cql3Type.toCQLLiteral(el.value, version));
                }
            }
            expected.append(bracketClose);
            buffer = CollectionSerializer.pack(buffers, added.size(), version);
        }

        return new Value(expected.toString(), collectionType.asCQL3Type(), buffer);
    }

    /**
     * Generates a value for any type or type structure.
     */
    static Value generateAnyValue(int version, CQL3Type type)
    {
        if (type instanceof CQL3Type.Native)
            return generateNativeValue(type, false);
        if (type instanceof CQL3Type.Tuple)
            return generateTupleValue(version, (TupleType) type.getType(), false);
        if (type instanceof CQL3Type.UserDefined)
            return generateUserDefinedValue(version, (UserType) type.getType(), false);
        if (type instanceof CQL3Type.Collection)
            return generateCollectionValue(version, (CollectionType) type.getType(), false);
        throw new AssertionError();
    }

    static Value generateTupleValue(int version, TupleType tupleType, boolean allowNull)
    {
        StringBuilder expected = new StringBuilder();
        ByteBuffer buffer;

        if (allowNull && randBool(0.05d))
        {
            // generate 'null' collection
            expected.append("null");
            buffer = null;
        }
        else
        {
            expected.append('(');

            // # of fields in this value
            int fields = tupleType.size();
            if (randBool(0.2d))
                fields = randInt(fields);

            ByteBuffer[] buffers = new ByteBuffer[fields];
            for (int i = 0; i < fields; i++)
            {
                AbstractType<?> fieldType = tupleType.type(i);

                if (i > 0)
                    expected.append(", ");

                if (allowNull && randBool(.1))
                {
                    expected.append("null");
                    continue;
                }

                Value value = generateAnyValue(version, fieldType.asCQL3Type());
                expected.append(value.expected);
                buffers[i] = value.value.duplicate();
            }
            expected.append(')');
            buffer = TupleType.buildValue(buffers);
        }

        return new Value(expected.toString(), tupleType.asCQL3Type(), buffer);
    }

    static Value generateUserDefinedValue(int version, UserType userType, boolean allowNull)
    {
        StringBuilder expected = new StringBuilder();
        ByteBuffer buffer;

        if (allowNull && randBool(0.05d))
        {
            // generate 'null' collection
            expected.append("null");
            buffer = null;
        }
        else
        {
            expected.append('{');

            // # of fields in this value
            int fields = userType.size();
            if (randBool(0.2d))
                fields = randInt(fields);

            ByteBuffer[] buffers = new ByteBuffer[fields];
            for (int i = 0; i < fields; i++)
            {
                AbstractType<?> fieldType = userType.type(i);

                if (i > 0)
                    expected.append(", ");

                expected.append(ColumnIdentifier.maybeQuote(userType.fieldNameAsString(i)));
                expected.append(": ");

                if (randBool(.1))
                {
                    expected.append("null");
                    continue;
                }

                Value value = generateAnyValue(version, fieldType.asCQL3Type());
                expected.append(value.expected);
                buffers[i] = value.value.duplicate();
            }
            expected.append('}');
            buffer = TupleType.buildValue(buffers);
        }

        return new Value(expected.toString(), userType.asCQL3Type(), buffer);
    }

    static Value generateNativeValue(CQL3Type type, boolean allowNull)
    {
        List<Value> values = nativeTypeValues.get(type);
        assert values != null : type.toString() + " needs to be defined";
        while (true)
        {
            Value v = values.get(randInt(values.size()));
            if (allowNull || v.value != null)
                return v;
        }
    }

    static CollectionType randomCollectionType(int level)
    {
        CollectionType.Kind kind = CollectionType.Kind.values()[randInt(CollectionType.Kind.values().length)];
        switch (kind)
        {
            case LIST:
            case SET:
                return ListType.getInstance(randomNestedType(level), randBool());
            case MAP:
                return MapType.getInstance(randomNestedType(level), randomNestedType(level), randBool());
        }
        throw new AssertionError();
    }

    static TupleType randomTupleType(int level)
    {
        int typeCount = 2 + randInt(5);
        List<AbstractType<?>> types = new ArrayList<>();
        for (int i = 0; i < typeCount; i++)
            types.add(randomNestedType(level));
        return new TupleType(types);
    }

    static UserType randomUserType(int level)
    {
        int typeCount = 2 + randInt(5);
        List<ByteBuffer> names = new ArrayList<>();
        List<AbstractType<?>> types = new ArrayList<>();
        for (int i = 0; i < typeCount; i++)
        {
            names.add(UTF8Type.instance.fromString('f' + randLetters(i)));
            types.add(randomNestedType(level));
        }
        return new UserType("ks", UTF8Type.instance.fromString("u" + randInt(1000000)), names, types, true);
    }

    //
    // Following methods are just helper methods. Mostly to generate many kinds of random values.
    //

    private static String randLetters(int len)
    {
        StringBuilder sb = new StringBuilder(len);
        while (len-- > 0)
        {
            int i = randInt(52);
            if (i < 26)
                sb.append((char) ('A' + i));
            else
                sb.append((char) ('a' + i - 26));
        }
        return sb.toString();
    }

    static AbstractType randomNativeType()
    {
        while (true)
        {
            CQL3Type.Native t = CQL3Type.Native.values()[randInt(CQL3Type.Native.values().length)];
            if (t != CQL3Type.Native.EMPTY)
                return t.getType();
        }
    }

    static boolean randBool()
    {
        return randBool(0.5d);
    }

    static boolean randBool(double probability)
    {
        return ThreadLocalRandom.current().nextDouble() < probability;
    }

    static long randLong()
    {
        return ThreadLocalRandom.current().nextLong();
    }

    static long randLong(long max)
    {
        return ThreadLocalRandom.current().nextLong(max);
    }

    static int randInt()
    {
        return ThreadLocalRandom.current().nextInt();
    }

    static int randInt(int max)
    {
        return ThreadLocalRandom.current().nextInt(max);
    }

    static short randShort()
    {
        return (short) ThreadLocalRandom.current().nextInt();
    }

    static byte randByte()
    {
        return (byte) ThreadLocalRandom.current().nextInt();
    }

    static double randDouble()
    {
        return ThreadLocalRandom.current().nextDouble();
    }

    static float randFloat()
    {
        return ThreadLocalRandom.current().nextFloat();
    }

    static String randString(boolean ascii)
    {
        int l = randInt(20);
        StringBuilder sb = new StringBuilder(l);
        for (int i = 0; i < l; i++)
        {
            if (randBool(.05))
                sb.append('\'');
            else
            {
                char c = (char) (ascii ? randInt(128) : randShort());
                sb.append(c);
            }
        }
        return UTF8Serializer.instance.deserialize(UTF8Serializer.instance.serialize(sb.toString()));
    }

    static ByteBuffer randBytes()
    {
        int l = randInt(20);
        byte[] v = new byte[l];
        for (int i = 0; i < l; i++)
        {
            v[i] = randByte();
        }
        return ByteBuffer.wrap(v);
    }

    private static String quote(String v)
    {
        return '\'' + QUOTE.matcher(v).replaceAll("''") + '\'';
    }
}
