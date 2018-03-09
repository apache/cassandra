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

import java.util.List;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * A single bind marker.
 */
public abstract class AbstractMarker extends Term.NonTerminal
{
    protected final int bindIndex;
    protected final ColumnSpecification receiver;

    protected AbstractMarker(int bindIndex, ColumnSpecification receiver)
    {
        this.bindIndex = bindIndex;
        this.receiver = receiver;
    }

    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        boundNames.add(bindIndex, receiver);
    }

    public boolean containsBindMarker()
    {
        return true;
    }

    public void addFunctionsTo(List<Function> functions)
    {
    }

    /**
     * A parsed, but non prepared, bind marker.
     */
    public static class Raw extends Term.Raw
    {
        protected final int bindIndex;

        public Raw(int bindIndex)
        {
            this.bindIndex = bindIndex;
        }

        public NonTerminal prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            if (receiver.type.isCollection())
            {
                switch (((CollectionType) receiver.type).kind)
                {
                    case LIST:
                        return new Lists.Marker(bindIndex, receiver);
                    case SET:
                        return new Sets.Marker(bindIndex, receiver);
                    case MAP:
                        return new Maps.Marker(bindIndex, receiver);
                    default:
                        throw new AssertionError();
                }
            }
            else if (receiver.type.isUDT())
            {
                return new UserTypes.Marker(bindIndex, receiver);
            }

            return new Constants.Marker(bindIndex, receiver);
        }

        @Override
        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        }

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

    /** A MultiColumnRaw version of AbstractMarker.Raw */
    public static abstract class MultiColumnRaw extends Term.MultiColumnRaw
    {
        protected final int bindIndex;

        public MultiColumnRaw(int bindIndex)
        {
            this.bindIndex = bindIndex;
        }

        public NonTerminal prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            throw new AssertionError("MultiColumnRaw..prepare() requires a list of receivers");
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        }

        @Override
        public String getText()
        {
            return "?";
        }
    }

    /**
     * A raw placeholder for multiple values of the same type for a single column.
     * For example, "SELECT ... WHERE user_id IN ?'.
     *
     * Because a single type is used, a List is used to represent the values.
     */
    public static final class INRaw extends Raw
    {
        public INRaw(int bindIndex)
        {
            super(bindIndex);
        }

        private static ColumnSpecification makeInReceiver(ColumnSpecification receiver)
        {
            ColumnIdentifier inName = new ColumnIdentifier("in(" + receiver.name + ")", true);
            return new ColumnSpecification(receiver.ksName, receiver.cfName, inName, ListType.getInstance(receiver.type, false));
        }

        @Override
        public Lists.Marker prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            return new Lists.Marker(bindIndex, makeInReceiver(receiver));
        }
    }
}
