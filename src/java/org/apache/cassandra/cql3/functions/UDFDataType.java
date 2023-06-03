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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.base.Objects;
import com.google.common.reflect.TypeToken;

import org.apache.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.cql3.functions.types.TypeCodec;
import org.apache.cassandra.cql3.functions.types.TypeCodec.*;
import org.apache.cassandra.cql3.functions.types.exceptions.InvalidTypeException;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JavaDriverUtils;

/**
 * Represents a data type used within the UDF/UDA.
 * </p>
 * Internaly, UDFs and UDAs use the Java driver types. This class maintains the mapping between the C* type
 * and the corresponding java driver type.
 */
public final class UDFDataType
{
    private static final Pattern JAVA_LANG_PREFIX = Pattern.compile("\\bjava\\.lang\\.");

    /**
     * The Cassandra type.
     */
    private final AbstractType<?> abstractType;

    /**
     * The Java driver type used to serialize and deserialize the UDF/UDA data.
     */
    private final TypeCodec<?> typeCodec;

    /**
     * The java type corresponding to this data type.
     */
    private final TypeToken<?> javaType;

    /**
     * Creates a new {@code UDFDataType} corresponding to the specified Cassandra type.
     *
     * @param abstractType the Cassandra type
     * @param usePrimitive {@code true} if the primitive type can be used.
     */
    private UDFDataType(AbstractType<?> abstractType, boolean usePrimitive)
    {
        this.abstractType = abstractType;
        this.typeCodec = JavaDriverUtils.codecFor(abstractType);
        TypeToken<?> token = typeCodec.getJavaType();
        this.javaType = usePrimitive ? token.unwrap() : token;
    }

    /**
     * Converts this type into the corresponding Cassandra type.
     * @return the corresponding Cassandra type
     */
    public AbstractType<?> toAbstractType()
    {
        return abstractType;
    }

    /**
     * Converts this type into the corresponding Java class.
     * @return the corresponding Java class.
     */
    public Class<?> toJavaClass()
    {
        return typeCodec.getJavaType().getRawType();
    }

    /**
     * @return the name of the java type corresponding to this class.
     */
    public String getJavaTypeName()
    {
        String n = javaType.toString();
        return JAVA_LANG_PREFIX.matcher(n).replaceAll("");
    }

    /**
     * Checks if this type is corresponding to a Java primitive type.
     * @return {@code true} if this type is corresponding to a Java primitive type, {@code false} otherwise.
     */
    public boolean isPrimitive()
    {
        return javaType.isPrimitive();
    }

    /**
     * Returns the {@code UDFDataType} corresponding to the specified type.
     *
     * @param abstractType the Cassandra type
     * @param usePrimitive {@code true} if the primitive type can be used.
     * @return the {@code UDFDataType} corresponding to the specified type.
     */
    public static UDFDataType wrap(AbstractType<?> abstractType, boolean usePrimitive)
    {
        return new UDFDataType(abstractType, usePrimitive);
    }

    /**
     * Returns the {@code UDFDataType}s corresponding to the specified types.
     *
     * @param argTypes the Cassandra types
     * @param usePrimitive {@code true} if the primitive types can be used.
     * @return the {@code UDFDataType}s corresponding to the specified types.
     */
    public static List<UDFDataType> wrap(List<AbstractType<?>> argTypes, boolean usePrimitive)
    {
        List<UDFDataType> types = new ArrayList<>(argTypes.size());
        for (AbstractType<?> argType : argTypes)
        {
            types.add(UDFDataType.wrap(argType, usePrimitive));
        }
        return types;
    }

    /**
     * Converts this type into the corresponding Java driver type.
     * @return the corresponding Java driver type.
     */
    public DataType toDataType()
    {
        return typeCodec.getCqlType();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(abstractType, javaType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof UDFDataType))
            return false;

        UDFDataType that = (UDFDataType) obj;

        return abstractType.equals(that.abstractType) && javaType.equals(that.javaType);
    }

    /**
     * Deserializes the specified oject.
     *
     * @param protocolVersion the protocol version
     * @param buffer the serialized object
     * @return the deserialized object
     */
    public Object compose(ProtocolVersion protocolVersion, ByteBuffer buffer)
    {
        if (buffer == null || (buffer.remaining() == 0 && abstractType.isEmptyValueMeaningless()))
            return null;

        return typeCodec.deserialize(buffer, protocolVersion);
    }

    /**
     * Serialized the specified oject.
     *
     * @param protocolVersion the protocol version
     * @param value the value to serialize
     * @return the serialized object
     */
    @SuppressWarnings("unchecked")
    public ByteBuffer decompose(ProtocolVersion protocolVersion, Object value)
    {
        if (value == null)
            return null;

        if (!toJavaClass().isAssignableFrom(value.getClass()))
            throw new InvalidTypeException("Invalid value for CQL type " + toDataType().getName());

        return ((TypeCodec<Object>) typeCodec).serialize(value, protocolVersion);
    }

    /**
     * Serialized the specified byte.
     *
     * @param protocolVersion the protocol version
     * @param value the value to serialize
     * @return the serialized byte
     */
    public ByteBuffer decompose(ProtocolVersion protocolVersion, byte value)
    {
        if (!(typeCodec instanceof PrimitiveByteCodec))
            throw new InvalidTypeException("Invalid value for CQL type " + toDataType().getName());

        return ((PrimitiveByteCodec) typeCodec).serializeNoBoxing(value, protocolVersion);
    }

    /**
     * Serialized the specified byte.
     *
     * @param protocolVersion the protocol version
     * @param value the value to serialize
     * @return the serialized byte
     */
    public ByteBuffer decompose(ProtocolVersion protocolVersion, short value)
    {
        if (!(typeCodec instanceof PrimitiveShortCodec))
            throw new InvalidTypeException("Invalid value for CQL type " + toDataType().getName());

        return ((PrimitiveShortCodec) typeCodec).serializeNoBoxing(value, protocolVersion);
    }

    /**
     * Serialized the specified byte.
     *
     * @param protocolVersion the protocol version
     * @param value the value to serialize
     * @return the serialized byte
     */
    public ByteBuffer decompose(ProtocolVersion protocolVersion, int value)
    {
        if (!(typeCodec instanceof PrimitiveIntCodec))
            throw new InvalidTypeException("Invalid value for CQL type " + toDataType().getName());

        return ((PrimitiveIntCodec) typeCodec).serializeNoBoxing(value, protocolVersion);
    }

    /**
     * Serialized the specified byte.
     *
     * @param protocolVersion the protocol version
     * @param value the value to serialize
     * @return the serialized byte
     */
    public ByteBuffer decompose(ProtocolVersion protocolVersion, long value)
    {
        if (!(typeCodec instanceof PrimitiveLongCodec))
            throw new InvalidTypeException("Invalid value for CQL type " + toDataType().getName());

        return ((PrimitiveLongCodec) typeCodec).serializeNoBoxing(value, protocolVersion);
    }

    /**
     * Serialized the specified byte.
     *
     * @param protocolVersion the protocol version
     * @param value the value to serialize
     * @return the serialized byte
     */
    public ByteBuffer decompose(ProtocolVersion protocolVersion, float value)
    {
        if (!(typeCodec instanceof PrimitiveFloatCodec))
            throw new InvalidTypeException("Invalid value for CQL type " + toDataType().getName());

        return ((PrimitiveFloatCodec) typeCodec).serializeNoBoxing(value, protocolVersion);
    }

    /**
     * Serialized the specified byte.
     *
     * @param protocolVersion the protocol version
     * @param value the value to serialize
     * @return the serialized byte
     */
    public ByteBuffer decompose(ProtocolVersion protocolVersion, double value)
    {
        if (!(typeCodec instanceof PrimitiveDoubleCodec))
            throw new InvalidTypeException("Invalid value for CQL type " + toDataType().getName());

        return ((PrimitiveDoubleCodec) typeCodec).serializeNoBoxing(value, protocolVersion);
    }

    public ArgumentDeserializer getArgumentDeserializer()
    {
        // If the type is corresponding to a primitive one we can use the ArgumentDeserializer of the AbstractType
        if (isPrimitive())
            return abstractType.getArgumentDeserializer();

        return this::compose;
    }
}
