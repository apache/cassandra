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
import java.util.*;

import org.apache.cassandra.cql3.Term.MultiItemTerminal;
import org.apache.cassandra.cql3.Term.Terminal;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * A set of {@code Terms}
 */
public interface Terms
{
    /**
     * The {@code List} returned when the list was not set.
     */
    @SuppressWarnings("rawtypes")
    public static final List UNSET_LIST = new AbstractList()
    {
        @Override
        public Object get(int index)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size()
        {
            return 0;
        }
    };

    /**
     * Adds all functions (native and user-defined) used by any of the terms to the specified list.
     * @param functions the list to add to
     */
    public void addFunctionsTo(List<Function> functions);

    /**
     * Collects the column specifications for the bind variables in the terms.
     * This is obviously a no-op if the terms are Terminal.
     *
     * @param boundNames the variables specification where to collect the
     * bind variables of the terms in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames);

    /**
     * Bind the values in these terms to the values contained in {@code options}.
     * This is obviously a no-op if the term is Terminal.
     *
     * @param options the values to bind markers to.
     * @return the result of binding all the variables of these NonTerminals or an {@code UNSET_LIST} if the term
     * was unset.
     */
    public List<Terminal> bind(QueryOptions options);


    public List<ByteBuffer> bindAndGet(QueryOptions options);

    /**
     * Creates a {@code Terms} for the specified list marker.
     *
     * @param marker the list  marker
     * @param type the element type
     * @return a {@code Terms} for the specified list marker
     */
    public static Terms ofListMarker(final Lists.Marker marker, final AbstractType<?> type)
    {
        return new Terms()
        {
            @Override
            public void addFunctionsTo(List<Function> functions)
            {
            }

            @Override
            public void collectMarkerSpecification(VariableSpecifications boundNames)
            {
                marker.collectMarkerSpecification(boundNames);
            }

            @Override
            public List<ByteBuffer> bindAndGet(QueryOptions options)
            {
                Terminal terminal = marker.bind(options);

                if (terminal == null)
                    return null;

                if (terminal == Constants.UNSET_VALUE)
                    return UNSET_LIST;

                return ((MultiItemTerminal) terminal).getElements();
            }

            @Override
            public List<Terminal> bind(QueryOptions options)
            {
                Terminal terminal = marker.bind(options);

                if (terminal == null)
                    return null;

                if (terminal == Constants.UNSET_VALUE)
                    return UNSET_LIST;

                java.util.function.Function<ByteBuffer, Term.Terminal> deserializer = deserializer(options.getProtocolVersion());

                List<ByteBuffer> boundValues = ((MultiItemTerminal) terminal).getElements();
                List<Term.Terminal> values = new ArrayList<>(boundValues.size());
                for (int i = 0, m = boundValues.size(); i < m; i++)
                {
                    ByteBuffer buffer = boundValues.get(i);
                    Term.Terminal value = buffer == null ? null : deserializer.apply(buffer);
                    values.add(value);
                }
                return values;
            }

            public java.util.function.Function<ByteBuffer, Term.Terminal> deserializer(ProtocolVersion version)
            {
                if (type.isCollection())
                {
                    switch (((CollectionType<?>) type).kind)
                    {
                        case LIST:
                            return e -> Lists.Value.fromSerialized(e, (ListType<?>) type);
                        case SET:
                            return e -> Sets.Value.fromSerialized(e, (SetType<?>) type);
                        case MAP:
                            return e -> Maps.Value.fromSerialized(e, (MapType<?, ?>) type);
                    }
                    throw new AssertionError();
                }
                return e -> new Constants.Value(e);
            }
        };
    }

    /**
     * Creates a {@code Terms} containing a single {@code Term}.
     *
     * @param term the {@code Term}
     * @return a {@code Terms} containing a single {@code Term}.
     */
    public static Terms of(final Term term)
    {
        return new Terms()
                {
                    @Override
                    public void addFunctionsTo(List<Function> functions)
                    {
                        term.addFunctionsTo(functions);
                    }

                    @Override
                    public void collectMarkerSpecification(VariableSpecifications boundNames)
                    {
                        term.collectMarkerSpecification(boundNames);
                    }

                    @Override
                    public List<ByteBuffer> bindAndGet(QueryOptions options)
                    {
                        return Collections.singletonList(term.bindAndGet(options));
                    }

                    @Override
                    public List<Terminal> bind(QueryOptions options)
                    {
                        return Collections.singletonList(term.bind(options));
                    }
                };
    }

    /**
     * Creates a {@code Terms} containing a set of {@code Term}.
     *
     * @param term the {@code Term}
     * @return a {@code Terms} containing a set of {@code Term}.
     */
    public static Terms of(final List<Term> terms)
    {
        return new Terms()
                {
                    @Override
                    public void addFunctionsTo(List<Function> functions)
                    {
                        addFunctions(terms, functions);
                    }

                    @Override
                    public void collectMarkerSpecification(VariableSpecifications boundNames)
                    {
                        for (int i = 0, m = terms.size(); i <m; i++)
                        {
                            Term term = terms.get(i);
                            term.collectMarkerSpecification(boundNames);
                        }
                    }

                    @Override
                    public List<Terminal> bind(QueryOptions options)
                    {
                        int size = terms.size();
                        List<Terminal> terminals = new ArrayList<>(size);
                        for (int i = 0; i < size; i++)
                        {
                            Term term = terms.get(i);
                            terminals.add(term.bind(options));
                        }
                        return terminals;
                    }

                    @Override
                    public List<ByteBuffer> bindAndGet(QueryOptions options)
                    {
                        int size = terms.size();
                        List<ByteBuffer> buffers = new ArrayList<>(size);
                        for (int i = 0; i < size; i++)
                        {
                            Term term = terms.get(i);
                            buffers.add(term.bindAndGet(options));
                        }
                        return buffers;
                    }
                };
    }

    /**
     * Adds all functions (native and user-defined) of the specified terms to the list.
     * @param functions the list to add to
     */
    public static void addFunctions(Iterable<Term> terms, List<Function> functions)
    {
        for (Term term : terms)
        {
            if (term != null)
                term.addFunctionsTo(functions);
        }
    }

    public static ByteBuffer asBytes(String keyspace, String term, AbstractType type)
    {
        ColumnSpecification receiver = new ColumnSpecification(keyspace, SchemaConstants.DUMMY_KEYSPACE_OR_TABLE_NAME, new ColumnIdentifier("(dummy)", true), type);
        Term.Raw rawTerm = CQLFragmentParser.parseAny(CqlParser::term, term, "CQL term");
        return rawTerm.prepare(keyspace, receiver).bindAndGet(QueryOptions.DEFAULT);
    }
}
