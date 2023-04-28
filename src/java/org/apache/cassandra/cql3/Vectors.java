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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.Constants.UNSET_VALUE;

public class Vectors
{
    public static class Setter extends Operation
    {
        public Setter(ColumnMetadata column, Term t)
        {
            super(column, t);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            ByteBuffer value = t.bindAndGet(params.options);
            if (value == null)
                params.addTombstone(column);
            else if (value != ByteBufferUtil.UNSET_BYTE_BUFFER) // use reference equality and not object equality
                params.addCell(column, value);
        }
    }

    public static class Value extends Term.Terminal
    {
        public final List<ByteBuffer> elements;

        public Value(List<ByteBuffer> elements)
        {
            this.elements = elements;
        }

        public static Value fromSerialized(ByteBuffer value, ProtocolVersion version) throws InvalidRequestException
        {
            try
            {
                List<Double> l = ListSerializer.getInstance(DoubleType.instance.getSerializer())
                                               .deserializeForNativeProtocol(value, ByteBufferAccessor.instance, version);

                List<ByteBuffer> elements = new ArrayList<>();
                for (Double element : l)
                {
                    elements.add(element == null ? null : FloatType.instance.decompose((float)(double)element));
                }
                return new Value(elements);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }

        public ByteBuffer get(ProtocolVersion protocolVersion)
        {
            ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;
            var bb = ByteBuffer.allocate(Float.BYTES * elements.size());
            for (var v : elements)
                accessor.write(v, bb);
            return accessor.valueOf(bb.flip());
        }

        public boolean equals(ListType lt, Lists.Value v)
        {
            if (elements.size() != v.elements.size())
                return false;

            for (int i = 0; i < elements.size(); i++)
                if (lt.getElementsType().compare(elements.get(i), v.elements.get(i)) != 0)
                    return false;

            return true;
        }

        public List<ByteBuffer> getElements()
        {
            return elements;
        }
    }

    /**
     * Basically similar to a Value, but with some non-pure function (that need
     * to be evaluated at execution time) in it.
     *
     * Note: this would also work for a list with bind markers, but we don't support
     * that because 1) it's not excessively useful and 2) we wouldn't have a good
     * column name to return in the ColumnSpecification for those markers (not a
     * blocker per-se but we don't bother due to 1)).
     */
    public static class DelayedValue extends Term.NonTerminal
    {
        private final List<Term> elements;

        public DelayedValue(List<Term> elements)
        {
            this.elements = elements;
        }

        public boolean containsBindMarker()
        {
            // False since we don't support them in collection
            return false;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
        }

        public Terminal bind(QueryOptions options) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<>(elements.size());
            for (Term t : elements)
            {
                ByteBuffer bytes = t.bindAndGet(options);

                if (bytes == null)
                    throw new InvalidRequestException("null is not supported inside collections");
                if (bytes == ByteBufferUtil.UNSET_BYTE_BUFFER)
                    return UNSET_VALUE;

                buffers.add(bytes);
            }
            return new Value(buffers);
        }

        public void addFunctionsTo(List<Function> functions)
        {
            Terms.addFunctions(elements, functions);
        }
    }

    public static class Marker extends AbstractMarker
    {
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof VectorType;
        }

        public Terminal bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer value = options.getValues().get(bindIndex);
            if (value == null)
                return null;
            if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
                return UNSET_VALUE;
            return Value.fromSerialized(value, options.getProtocolVersion());
        }
    }
}
