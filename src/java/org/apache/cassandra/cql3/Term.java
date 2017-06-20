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
import java.util.List;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * A CQL3 term, i.e. a column value with or without bind variables.
 *
 * A Term can be either terminal or non terminal. A term object is one that is typed and is obtained
 * from a raw term (Term.Raw) by poviding the actual receiver to which the term is supposed to be a
 * value of.
 */
public interface Term
{
    /**
     * Collects the column specification for the bind variables in this Term.
     * This is obviously a no-op if the term is Terminal.
     *
     * @param boundNames the variables specification where to collect the
     * bind variables of this term in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames);

    /**
     * Bind the values in this term to the values contained in {@code values}.
     * This is obviously a no-op if the term is Terminal.
     *
     * @param options the values to bind markers to.
     * @return the result of binding all the variables of this NonTerminal (or
     * 'this' if the term is terminal).
     */
    public Terminal bind(QueryOptions options) throws InvalidRequestException;

    /**
     * A shorter for bind(values).get().
     * We expose it mainly because for constants it can avoids allocating a temporary
     * object between the bind and the get (note that we still want to be able
     * to separate bind and get for collections).
     */
    public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException;

    /**
     * Whether or not that term contains at least one bind marker.
     *
     * Note that this is slightly different from being or not a NonTerminal,
     * because calls to non pure functions will be NonTerminal (see #5616)
     * even if they don't have bind markers.
     */
    public abstract boolean containsBindMarker();

    /**
     * Whether that term is terminal (this is a shortcut for {@code this instanceof Term.Terminal}).
     */
    default public boolean isTerminal()
    {
        return false; // overriden below by Terminal
    }

    public void addFunctionsTo(List<Function> functions);

    /**
     * A parsed, non prepared (thus untyped) term.
     *
     * This can be one of:
     *   - a constant
     *   - a collection literal
     *   - a function call
     *   - a marker
     */
    public abstract class Raw implements AssignmentTestable
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
         * otherwise (for instance, the type isn't inferable for a bind marker. Even for
         * literals, the exact type is not inferrable since they are valid for many
         * different types and so this will return {@code null} too).
         */
        public abstract AbstractType<?> getExactTypeIfKnown(String keyspace);

        @Override
        public String toString()
        {
            return getText();
        }
    }

    public abstract class MultiColumnRaw extends Term.Raw
    {
        public abstract Term prepare(String keyspace, List<? extends ColumnSpecification> receiver) throws InvalidRequestException;
    }

    /**
     * A terminal term, one that can be reduced to a byte buffer directly.
     *
     * This includes most terms that don't have a bind marker (an exception
     * being delayed call for non pure function that are NonTerminal even
     * if they don't have bind markers).
     *
     * This can be only one of:
     *   - a constant value
     *   - a collection value
     *
     * Note that a terminal term will always have been type checked, and thus
     * consumer can (and should) assume so.
     */
    public abstract class Terminal implements Term
    {
        public void collectMarkerSpecification(VariableSpecifications boundNames) {}
        public Terminal bind(QueryOptions options) { return this; }

        public void addFunctionsTo(List<Function> functions)
        {
        }

        // While some NonTerminal may not have bind markers, no Term can be Terminal
        // with a bind marker
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
         * @param protocolVersion
         */
        public abstract ByteBuffer get(ProtocolVersion protocolVersion) throws InvalidRequestException;

        public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
        {
            return get(options.getProtocolVersion());
        }
    }

    public abstract class MultiItemTerminal extends Terminal
    {
        public abstract List<ByteBuffer> getElements();
    }

    /**
     * A non terminal term, i.e. a term that can only be reduce to a byte buffer
     * at execution time.
     *
     * We have the following type of NonTerminal:
     *   - marker for a constant value
     *   - marker for a collection value (list, set, map)
     *   - a function having bind marker
     *   - a non pure function (even if it doesn't have bind marker - see #5616)
     */
    public abstract class NonTerminal implements Term
    {
        public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
        {
            Terminal t = bind(options);
            return t == null ? null : t.get(options.getProtocolVersion());
        }
    }
}
