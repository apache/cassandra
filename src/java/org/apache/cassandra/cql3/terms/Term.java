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
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A CQL3 term, i.e. a column value with or without bind variables.
 * <p>
 * A Term can be either {@link Terminal} or {@link NonTerminal}. A term object is one that is typed and is obtained
 * from a raw term (Term.Raw) by providing the actual {@link ColumnSpecification} receiver to which the term is supposed to be a
 * value of.
 */
public interface Term
{
    /**
     * The {@code List} returned when the list was not set.
     */
    @SuppressWarnings("rawtypes")
    List UNSET_LIST = Collections.unmodifiableList(new AbstractList()
    {
        @Override
        public ByteBuffer get(int index)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size()
        {
            throw new UnsupportedOperationException();
        }
    });

    /**
     * Collects the column specification for the bind variables in this Term.
     * This is obviously a no-op if the term is Terminal.
     *
     * @param boundNames the variables specification where to collect the
     * bind variables of this term in.
     */
    void collectMarkerSpecification(VariableSpecifications boundNames);

    /**
     * Bind the values in this term to the values contained in the {@code options}.
     * This is obviously a no-op if the term is Terminal.
     *
     * @param options the values to bind markers to.
     * @return the {@code Terminal} resulting of binding the values contained in the {@code options}.
     */
    Terminal bind(QueryOptions options);

    /**
     * A shorter for {@code bind(options).get()}.
     * We expose it mainly because for constants it can avoid allocating a temporary
     * object between the bind and the get (note that we still want to be able
     * to separate bind and get for collections).
     */
    ByteBuffer bindAndGet(QueryOptions options);

    /**
     * A shorter for {@code bind(options).getElements()}.
     * We expose it mainly because for constants it can avoid allocating a temporary
     * object between the bind and the getElements.
     */
    List<ByteBuffer> bindAndGetElements(QueryOptions options);

    /**
     * Whether that term contains at least one bind marker.
     * <p>
     * Note that this is slightly different from being or not a NonTerminal,
     * because calls to non-pure functions will be {@code NonTerminal} (see #5616)
     * even if they don't have bind markers.
     */
    boolean containsBindMarker();

    /**
     * Whether that term is terminal (this is a shortcut for {@code this instanceof Term.Terminal}).
     */
    default boolean isTerminal()
    {
        return false; // overriden below by Terminal
    }

    /**
     * Adds the functions used by this {@link Term} to the list of functions.
     * <p>
     * This method is used to discover prepare statements function dependencies on schema updates.
     * @param functions the list of functions to add to
     */
    void addFunctionsTo(List<Function> functions);

    /**
     * Converts the term represented by the specified {@code String} into its binary representation.
     * @param term the term to convert
     * @param type the type of the term
     * @return the term binary representation
     */
    static ByteBuffer asBytes(String keyspace, String term, AbstractType<?> type)
    {
        ColumnSpecification receiver = new ColumnSpecification(keyspace, SchemaConstants.DUMMY_KEYSPACE_OR_TABLE_NAME, new ColumnIdentifier("(dummy)", true), type);
        Term.Raw rawTerm = CQLFragmentParser.parseAny(CqlParser::term, term, "CQL term");
        return rawTerm.prepare(keyspace, receiver).bindAndGet(QueryOptions.DEFAULT);
    }

    /**
     * A parsed, non prepared (thus untyped) term.
     * <p>
     * This can be one of:
     * <ul>
     *     <li>a constant</li>
     *     <li>a multi-element type literal</li>
     *     <li>a function call</li>
     *     <li>a marker</li>
     * </ul>
     */
    abstract class Raw implements AssignmentTestable
    {
        /**
         * This method validates this RawTerm is valid for provided column
         * specification and "prepare" this RawTerm, returning the resulting
         * prepared Term.
         *
         * @param receiver the "column" this RawTerm is supposed to be a value of. Note
         * that the ColumnSpecification may not correspond to a real column in the
         * case this RawTerm describe a list index or a map key, etc...
         * @return the prepared term.
         */
        public abstract Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException;

        /**
         * @return a String representation of the raw term that can be used when reconstructing a CQL query string.
         */
        public abstract String getText();

        /**
         * The type of the {@code term} if it can be infered.
         *
         * @param keyspace the keyspace on which the statement containing this term is on.
         * @return the type of this {@code Term} if inferrable, or {@code null}
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

        @Override
        public int hashCode()
        {
            return getText().hashCode();
        }

        @Override
        public boolean equals(Object o)
        {
            return this == o || (o instanceof Raw && getText().equals(((Raw) o).getText()));
        }
    }

    /**
     * A terminal term, one that can be reduced to a byte buffer directly.
     * <p>
     * This includes most terms that don't have a bind marker (an exception
     * being delayed call for non-pure function that are NonTerminal even
     * if they don't have bind markers).
     * <p>
     * This can be only one of:
     *   <ul>
     *     <li>a constant value</li>
     *     <li>a multi-element value</li>
     *   </ul>
     * <p>
     * Note that a terminal term will always have been type checked, and thus
     * consumer can (and should) assume so.
     */
    abstract class Terminal implements Term
    {
        @Override
        public void collectMarkerSpecification(VariableSpecifications boundNames) {}

        @Override
        public Terminal bind(QueryOptions options) { return this; }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
        }

        // While some NonTerminal may not have bind markers, no Term can be Terminal
        // with a bind marker
        @Override
        public boolean containsBindMarker()
        {
            return false;
        }

        @Override
        public boolean isTerminal()
        {
            return true;
        }

        /**
         * @return the serialized value of this terminal.
         */
        public abstract ByteBuffer get();

        /**
         * Returns the serialized values of this Term elements, if this term represents a Collection, Tuple or UDT.
         * If this term does not represent a multi-elements type it will return a list containing the serialized value
         * of this terminal
         * @return a list containing serialized values of this Term elements
         */
        public List<ByteBuffer> getElements()
        {
            ByteBuffer value = get();
            return value == null ? Collections.emptyList()
                                 : value == ByteBufferUtil.UNSET_BYTE_BUFFER ? UNSET_LIST
                                                                             : Collections.singletonList(value);
        }

        @Override
        public ByteBuffer bindAndGet(QueryOptions options)
        {
            return get();
        }

        @Override
        public List<ByteBuffer> bindAndGetElements(QueryOptions options)
        {
            return getElements();
        }
    }

    /**
     * A non-terminal term, i.e. a term that can only be reduced to a byte buffer
     * at execution time.
     * <p>
     * We have the following type of NonTerminal:
     * <ul>
     *   <li>marker for a constant value</li>
     *   <li>marker for a multi-element data type value (list, set, map, tuple, udt, vector)</li>
     *   <li>a function having bind markers</li>
     *   <li>a non-pure function (even if it doesn't have bind markers - see #5616)</li>
     * </ul>
     */
    abstract class NonTerminal implements Term
    {
        @Override
        public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
        {
            Terminal t = bind(options);
            return t == null ? null : t.get();
        }

        @Override
        public List<ByteBuffer> bindAndGetElements(QueryOptions options)
        {
            Terminal t = bind(options);
            return t == null ? Collections.emptyList() : t.getElements();
        }
    }
}
