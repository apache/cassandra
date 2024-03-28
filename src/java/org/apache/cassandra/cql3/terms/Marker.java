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
import java.util.List;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MultiElementType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A placeholder, also called bind marker, for a single value represented in CQL as {@code ?} for an unnamed marker or {@code :<name>} for a named marker.
 * For example, {@code SELECT ... WHERE pk = ?} or {@code SELECT ... WHERE pk = :myKey}.
 */
public final class Marker extends Term.NonTerminal
{
    /**
     * The index of the bind variable within the query options.
     * <p>When a query is executed the value of this placeholder is retrieved from the query options values using
     * the bindIndex. (see {@link Marker#bind(QueryOptions)}</p>
     */
    private final int bindIndex;

    private final ColumnSpecification receiver;

    private Marker(int bindIndex, ColumnSpecification receiver)
    {
        this.bindIndex = bindIndex;
        this.receiver = receiver;
    }

    @Override
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        boundNames.add(bindIndex, receiver);
    }

    @Override
    public boolean containsBindMarker()
    {
        return true;
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
    }

    @Override
    public Term.Terminal bind(QueryOptions options) throws InvalidRequestException
    {
        try
        {
            ByteBuffer bytes = options.getValues().get(bindIndex);
            if (bytes == null)
                return null;

            if (bytes == ByteBufferUtil.UNSET_BYTE_BUFFER)
                return Constants.UNSET_VALUE;

            if (receiver.type instanceof MultiElementType<?>)
                return MultiElements.Value.fromSerialized(bytes, (MultiElementType<?>) receiver.type);

            receiver.type.validate(bytes);
            return new Constants.Value(bytes);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage(), e);
        }
    }

    /**
     * A parsed, but non prepared, bind marker.
     */
    public static class Raw extends Term.Raw
    {
        /**
         * The index of the bind variable within the query options.
         */
        private final int bindIndex;

        public Raw(int bindIndex)
        {
            this.bindIndex = bindIndex;
        }

        @Override
        public NonTerminal prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            return new Marker(bindIndex, receiver);
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
    }
}
