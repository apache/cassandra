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

package org.apache.cassandra.cql3.terms;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MultiElementType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A CQL named or unnamed bind marker for an {@code IN} restriction.
 * For example, 'SELECT ... WHERE pk IN ?' or 'SELECT ... WHERE pk IN :myKey '.
 */
public final class InMarker extends Terms.NonTerminals
{
    private final int bindIndex;
    private final ColumnSpecification receiver;

    private InMarker(int bindIndex, ColumnSpecification receiver)
    {
        this.bindIndex = bindIndex;
        this.receiver = receiver;
    }

    @Override
    public void addFunctionsTo(List<Function> functions) {}

    @Override
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        boundNames.add(bindIndex, receiver);
    }

    @Override
    public Terminals bind(QueryOptions options)
    {
        ByteBuffer values = options.getValues().get(bindIndex);
        if (values == null)
            return null;

        if (values == ByteBufferUtil.UNSET_BYTE_BUFFER)
            return UNSET_TERMINALS;

        ListType<?> type = (ListType<?>) receiver.type;
        return toTerminals(values, type, terminalConverter(type.getElementsType()));
    }

    private <T> Terminals toTerminals(ByteBuffer value,
                                      ListType<T> type,
                                      java.util.function.Function<ByteBuffer, Term.Terminal> terminalConverter)
    {
        List<T> elements = type.getSerializer().deserialize(value, ByteBufferAccessor.instance);
        List<Term.Terminal> terminals = new ArrayList<>(elements.size());
        for (T element : elements)
        {
            terminals.add(element == null ? null : terminalConverter.apply(type.getElementsType().decompose(element)));
        }
        return Terminals.of(terminals);
    }

    public static java.util.function.Function<ByteBuffer, Term.Terminal> terminalConverter(AbstractType<?> type)
    {
        if (type instanceof MultiElementType<?>)
            return e -> MultiElements.Value.fromSerialized(e, (MultiElementType<?>) type);

        return Constants.Value::new;
    }

    @Override
    public List<ByteBuffer> bindAndGet(QueryOptions options)
    {
        Terminals terminals = bind(options);
        return terminals == null ? null : terminals.get();
    }

    @Override
    public List<List<ByteBuffer>> bindAndGetElements(QueryOptions options)
    {
        Terminals terminals = bind(options);
        return terminals == null ? null : terminals.getElements();
    }

    @Override
    public boolean containsSingleTerm()
    {
        return false;
    }

    /**
     * A raw placeholder for multiple values of the same type for a single column.
     * For example, {@code SELECT ... WHERE user_id IN ?}.
     * <p>
     * Because a single type is used, a List is used to represent the values.
     */
    public static final class Raw extends Terms.Raw
    {
        private final int bindIndex;

        public Raw(int bindIndex)
        {
            this.bindIndex = bindIndex;
        }

        @Override
        public InMarker prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            return new InMarker(bindIndex, makeInReceiver(receiver));
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return null;
        }

        @Override
        public String getText()
        {
            return "?";
        }

        private static ColumnSpecification makeInReceiver(ColumnSpecification receiver)
        {
            ColumnIdentifier inName = new ColumnIdentifier("in(" + receiver.name + ')', true);
            return new ColumnSpecification(receiver.ksName, receiver.cfName, inName, ListType.getInstance(receiver.type, false));
        }
   }
}