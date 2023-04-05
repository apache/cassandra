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
package org.apache.cassandra.cql3.functions.types;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.cql3.functions.types.exceptions.InvalidTypeException;

/**
 * A tuple type.
 *
 * <p>A tuple type is a essentially a list of types.
 */
public class TupleType extends DataType
{

    private final List<DataType> types;
    private final ProtocolVersion protocolVersion;
    private final CodecRegistry codecRegistry;

    TupleType(List<DataType> types, ProtocolVersion protocolVersion, CodecRegistry codecRegistry)
    {
        super(DataType.Name.TUPLE);
        this.types = ImmutableList.copyOf(types);
        this.protocolVersion = protocolVersion;
        this.codecRegistry = codecRegistry;
    }

    /**
     * Creates a "disconnected" tuple type (<b>you should prefer {@code
     * Metadata#newTupleType(DataType...) cluster.getMetadata().newTupleType(...)} whenever
     * possible</b>).
     *
     * <p>This method is only exposed for situations where you don't have a {@code Cluster} instance
     * available. If you create a type with this method and use it with a {@code Cluster} later, you
     * won't be able to set tuple fields with custom codecs registered against the cluster, or you
     * might get errors if the protocol versions don't match.
     *
     * @param protocolVersion the protocol version to use.
     * @param codecRegistry   the codec registry to use.
     * @param types           the types for the tuple type.
     * @return the newly created tuple type.
     */
    public static TupleType of(
    ProtocolVersion protocolVersion, CodecRegistry codecRegistry, DataType... types)
    {
        return new TupleType(Arrays.asList(types), protocolVersion, codecRegistry);
    }

    /**
     * The (immutable) list of types composing this tuple type.
     *
     * @return the (immutable) list of types composing this tuple type.
     */
    List<DataType> getComponentTypes()
    {
        return types;
    }

    /**
     * Returns a new empty value for this tuple type.
     *
     * @return an empty (with all component to {@code null}) value for this user type definition.
     */
    public TupleValue newValue()
    {
        return new TupleValue(this);
    }

    /**
     * Returns a new value for this tuple type that uses the provided values for the components.
     *
     * <p>The numbers of values passed to this method must correspond to the number of components in
     * this tuple type. The {@code i}th parameter value will then be assigned to the {@code i}th
     * component of the resulting tuple value.
     *
     * @param values the values to use for the component of the resulting tuple.
     * @return a new tuple values based on the provided values.
     * @throws IllegalArgumentException if the number of {@code values} provided does not correspond
     *                                  to the number of components in this tuple type.
     * @throws InvalidTypeException     if any of the provided value is not of the correct type for the
     *                                  component.
     */
    public TupleValue newValue(Object... values)
    {
        if (values.length != types.size())
            throw new IllegalArgumentException(
            String.format(
            "Invalid number of values. Expecting %d but got %d", types.size(), values.length));

        TupleValue t = newValue();
        for (int i = 0; i < values.length; i++)
        {
            DataType dataType = types.get(i);
            if (values[i] == null) t.setValue(i, null);
            else
                t.setValue(
                i, codecRegistry.codecFor(dataType, values[i]).serialize(values[i], protocolVersion));
        }
        return t;
    }

    @Override
    public boolean isFrozen()
    {
        return true;
    }

    /**
     * Return the protocol version that has been used to deserialize this tuple type, or that will be
     * used to serialize it. In most cases this should be the version currently in use by the cluster
     * instance that this tuple type belongs to, as reported by {@code
     * ProtocolOptions#getProtocolVersion()}.
     *
     * @return the protocol version that has been used to deserialize this tuple type, or that will be
     * used to serialize it.
     */
    ProtocolVersion getProtocolVersion()
    {
        return protocolVersion;
    }

    CodecRegistry getCodecRegistry()
    {
        return codecRegistry;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(new Object[]{ name, types });
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof TupleType)) return false;

        TupleType d = (TupleType) o;
        return name == d.name && types.equals(d.types);
    }

    /**
     * Return {@code true} if this tuple type contains the given tuple type, and {@code false}
     * otherwise.
     *
     * <p>A tuple type is said to contain another one if the latter has fewer components than the
     * former, but all of them are of the same type. E.g. the type {@code tuple<int, text>} is
     * contained by the type {@code tuple<int, text, double>}.
     *
     * <p>A contained type can be seen as a "partial" view of a containing type, where the missing
     * components are supposed to be {@code null}.
     *
     * @param other the tuple type to compare against the current one
     * @return {@code true} if this tuple type contains the given tuple type, and {@code false}
     * otherwise.
     */
    public boolean contains(TupleType other)
    {
        if (this.equals(other)) return true;
        if (other.types.size() > this.types.size()) return false;
        return types.subList(0, other.types.size()).equals(other.types);
    }

    @Override
    public String toString()
    {
        return "frozen<" + asFunctionParameterString() + '>';
    }

    @Override
    public String asFunctionParameterString()
    {
        StringBuilder sb = new StringBuilder();
        for (DataType type : types)
        {
            sb.append(sb.length() == 0 ? "tuple<" : ", ");
            sb.append(type.asFunctionParameterString());
        }
        return sb.append('>').toString();
    }
}
