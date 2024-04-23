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
import java.util.*;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.terms.Term.NonTerminal;
import org.apache.cassandra.cql3.terms.Term.Terminal;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * A set of {@code Terms}
 */
public interface Terms
{
    /**
     * The terminals returned when they were unset.
     */
    Terminals UNSET_TERMINALS = new Terminals()
    {
        @Override
        @SuppressWarnings("unchecked")
        public List<ByteBuffer> get()
        {
            return Term.UNSET_LIST;
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<List<ByteBuffer>> getElements()
        {
            return Term.UNSET_LIST;
        }

        @Override
        public List<Terminal> asList()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
        }

        @Override
        public boolean containsSingleTerm()
        {
            return false;
        }
    };

    /**
     * Adds all functions (native and user-defined) used by any of the terms to the specified list.
     * @param functions the list to add to
     */
    void addFunctionsTo(List<Function> functions);

    /**
     * Collects the column specifications for the bind variables in the terms.
     * This is obviously a no-op if the terms are Terminals.
     *
     * @param boundNames the variables specification where to collect the
     * bind variables of the terms in.
     */
    void collectMarkerSpecification(VariableSpecifications boundNames);

    /**
     * Bind the values in these terms to the values contained in {@code options}.
     * This is obviously a no-op if this {@code Terms} are Terminals.
     *
     * @param options the query options containing the values to bind markers to.
     * @return the result of binding all the variables of these NonTerminals.
     */
    Terminals bind(QueryOptions options);

    /**
     * A shorter for {@code bind(options).get()}.
     * We expose it mainly because for constants it can avoid allocating a temporary
     * object between the bind and the get.
     * @param options the query options containing the values to bind markers to.
     */
    List<ByteBuffer> bindAndGet(QueryOptions options);

    /**
     * A shorter for {@code bind(options).getElements()}.
     * We expose it mainly because for constants it can avoid allocating a temporary
     * object between the {@code bind} and the {@code getElements}.
     * @param options the query options containing the values to bind markers to.
     */
    List<List<ByteBuffer>> bindAndGetElements(QueryOptions options);

    /**
     * Creates a {@code Terms} containing a single {@code Term}.
     *
     * @param term the {@code Term}
     * @return a {@code Terms} containing a single {@code Term}.
     */
    static Terms of(final Term term)
    {
        if (term.isTerminal())
            return Terminals.of(term == Constants.NULL_VALUE ? null : (Terminal) term);

        return NonTerminals.of((NonTerminal) term);
    }

    /**
     * Creates a {@code Terms} containing a set of {@code Term}.
     *
     * @param terms the terms
     * @return a {@code Terms} containing a set of {@code Term}.
     */
    static Terms of(final List<Term> terms)
    {
        boolean allTerminals = terms.stream().allMatch(Term::isTerminal);

        if (allTerminals)
        {
            int size = terms.size();
            List<Terminal> terminals = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
            {
                Terminal terminal = (Terminal) terms.get(i);
                terminals.add(terminal == Constants.NULL_VALUE ? null : terminal);
            }
            return Terminals.of(terminals);
        }

        return NonTerminals.of(terms);
    }

    /**
     * Checks if these {@code terms} knows that it contains a single {@code term}.
     * <p>
     * If the instance is a marker it will not know how many terms it represents and will return false.
     * @return {@code true} if this {@code terms} know contains a single {@code term}, {@code false} otherwise.
     */
    boolean containsSingleTerm();

    /**
     * Adds all functions (native and user-defined) of the specified terms to the list.
     * @param functions the list to add to
     */
    static void addFunctions(Iterable<? extends Term> terms, List<Function> functions)
    {
        for (Term term : terms)
        {
            if (term != null)
                term.addFunctionsTo(functions);
        }
    }

    /**
     * A parsed, non prepared (thus untyped) set of terms.
     */
    abstract class Raw implements AssignmentTestable
    {
        /**
         * This method validates this {@code Terms.Raw} is valid for the provided column
         * specification and "prepare" this {@code Terms.Raw}, returning the resulting {@link Terms}.
         *
         * @param receiver the "column" the set of terms are supposed to be a value of. Note
         * that the ColumnSpecification may not correspond to a real column.
         * @return the prepared terms
         */
        public abstract Terms prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException;

        /**
         * @return a String representation of the raw terms that can be used when reconstructing a CQL query string.
         */
        public abstract String getText();

        /**
         * The type of the {@code Terms} if it can be inferred.
         *
         * @param keyspace the keyspace on which the statement containing these terms is on.
         * @return the type of this {@code Terms} if inferrable, or {@code null}
         * otherwise (for instance, the type isn't inferrable for a bind marker. Even for
         * literals, the exact type is not inferrable since they are valid for many
         * different types and so this will return {@code null} too).
         */
        public abstract AbstractType<?> getExactTypeIfKnown(String keyspace);

        @Override
        public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
        {
            return getExactTypeIfKnown(keyspace);
        }

        @Override
        public String toString()
        {
            return getText();
        }

        public static Raw of(List<? extends Term.Raw> raws)
        {
            return new Raw()
            {
                @Override
                public Terms prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
                {
                    List<Term> terms = new ArrayList<>(raws.size());
                    for (Term.Raw raw : raws)
                    {
                        terms.add(raw.prepare(keyspace, receiver));
                    }
                    return Terms.of(terms);
                }

                @Override
                public String getText()
                {
                    return raws.stream().map(Term.Raw::getText)
                                        .collect(Collectors.joining(", ", "(", ")"));
                }

                @Override
                public AbstractType<?> getExactTypeIfKnown(String keyspace)
                {
                    return null;
                }

                @Override
                public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
                {
                    return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                }
            };
        }

        public static Raw of(Term.Raw raw)
        {
            return new Raw()
            {
                @Override
                public Terms prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
                {
                    return Terms.of(raw.prepare(keyspace, receiver));
                }

                @Override
                public String getText()
                {
                    return raw.getText();
                }

                @Override
                public AbstractType<?> getExactTypeIfKnown(String keyspace)
                {
                    return raw.getExactTypeIfKnown(keyspace);
                }

                @Override
                public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
                {
                    return raw.testAssignment(keyspace, receiver);
                }
            };
        }
    }

    /**
     * Set of terms that contains only terminal terms.
     */
    abstract class Terminals implements Terms
    {
        @Override
        public void collectMarkerSpecification(VariableSpecifications boundNames) {}

        @Override
        public final Terminals bind(QueryOptions options)
        {
            return this;
        }

        @Override
        public List<ByteBuffer> bindAndGet(QueryOptions options)
        {
            return get();
        }

        @Override
        public List<List<ByteBuffer>> bindAndGetElements(QueryOptions options)
        {
            return getElements();
        }

        /**
         * @return the serialized values of this {@code Terminals}.
         */
        public abstract List<ByteBuffer> get();

        /**
         * Returns the serialized values of each Term elements, if these term represents a Collection, tuple, UDT or vector.
         * If the terms do not represent multi-elements type the method will return a list containing the serialized value of the terminals
         * @return a list containing serialized values of each Term elements
         */
        public abstract List<List<ByteBuffer>> getElements();

        /**
         * Converts these {@code Terminals} into a {@code List} of {@code Term.Terminal}.
         * @return a {@code List} of {@code Term.Terminal}.
         */
        public abstract List<Term.Terminal> asList();

        /**
         * Converts a {@code Terminal} into a {@code Terminals}.
         * @param terminal the {@code Terminal} to convert
         * @return a {@code Terminals}.
         */
        public static Terminals of(Terminal terminal)
        {
            return new Terminals()
            {
                @Override
                public List<ByteBuffer> get()
                {
                    return Collections.singletonList(terminal == null ? null : terminal.get());
                }

                @Override
                public List<List<ByteBuffer>> getElements()
                {
                    return Collections.singletonList(terminal == null ? null : terminal.getElements());
                }

                @Override
                public List<Terminal> asList()
                {
                    return Collections.singletonList(terminal);
                }

                @Override
                public void addFunctionsTo(List<Function> functions)
                {
                    if (terminal != null)
                        terminal.addFunctionsTo(functions);
                }

                @Override
                public boolean containsSingleTerm()
                {
                    return true;
                }
            };
        }

        public static Terminals of(List<Terminal> terminals)
        {
            return new Terminals()
            {
                @Override
                public List<Terminal> asList()
                {
                    return terminals;
                }

                @Override
                public List<ByteBuffer> get()
                {
                    int size = terminals.size();
                    List<ByteBuffer> buffers = new ArrayList<>(size);
                    for (int i = 0; i < size; i++)
                    {
                        Terminal terminal = terminals.get(i);
                        buffers.add(terminal == null ? null : terminal.get());
                    }
                    return buffers;
                }

                @Override
                public List<List<ByteBuffer>> getElements()
                {
                    int size = terminals.size();
                    List<List<ByteBuffer>> buffers = new ArrayList<>(size);
                    for (int i = 0; i < size; i++)
                    {
                        Terminal terminal = terminals.get(i);
                        buffers.add(terminal == null ? null : terminal.getElements());
                    }
                    return buffers;
                }

                @Override
                public void addFunctionsTo(List<Function> functions)
                {
                    addFunctions(terminals, functions);
                }

                @Override
                public boolean containsSingleTerm()
                {
                    return terminals.size() == 1;
                }
            };
        }
    }

    /**
     * Set of terms that contains at least one non-terminal term.
     */
    abstract class NonTerminals implements Terms
    {
        public static NonTerminals of(NonTerminal term)
        {
            return new NonTerminals()
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
                public Terminals bind(QueryOptions options)
                {
                    return Terminals.of(term.bind(options));
                }

                @Override
                public List<ByteBuffer> bindAndGet(QueryOptions options)
                {
                    return Collections.singletonList(term.bindAndGet(options));
                }

                @Override
                public List<List<ByteBuffer>> bindAndGetElements(QueryOptions options)
                {
                    return Collections.singletonList(term.bindAndGetElements(options));
                }

                @Override
                public boolean containsSingleTerm()
                {
                    return true;
                }

                @Override
                public String toString()
                {
                    return term.toString();
                }
            };
        }

        public static NonTerminals of(List<Term> terms)
        {
            return new NonTerminals()
            {
                @Override
                public void addFunctionsTo(List<Function> functions)
                {
                    addFunctions(terms, functions);
                }

                @Override
                public void collectMarkerSpecification(VariableSpecifications boundNames)
                {
                    for (int i = 0, m = terms.size(); i < m; i++)
                    {
                        Term term = terms.get(i);
                        term.collectMarkerSpecification(boundNames);
                    }
                }

                @Override
                public Terminals bind(QueryOptions options)
                {
                    int size = terms.size();
                    List<Terminal> terminals = new ArrayList<>(size);
                    for (int i = 0; i < size; i++)
                    {
                        Term term = terms.get(i);
                        terminals.add(term.bind(options));
                    }
                    return Terminals.of(terminals);
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

                @Override
                public List<List<ByteBuffer>> bindAndGetElements(QueryOptions options)
                {
                    int size = terms.size();
                    List<List<ByteBuffer>> buffers = new ArrayList<>(size);
                    for (int i = 0; i < size; i++)
                    {
                        Term term = terms.get(i);
                        buffers.add(term.bindAndGetElements(options));
                    }
                    return buffers;
                }

                @Override
                public boolean containsSingleTerm()
                {
                    return terms.size() == 1;
                }

                @Override
                public String toString()
                {
                    return terms.stream().map(Objects::toString).collect(Collectors.joining(", ", "(", ")"));
                }
            };
        }
    }
}
