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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Represents {@code [a, b, c, d]} in CQL.  Mutliple {@link org.apache.cassandra.db.marshal.AbstractType} use array literals,
 * so this class is meant to act as a proxy once the real type is known.
 */
public class ArrayLiteral extends Term.Raw
{
    private final List<Term.Raw> elements;

    public ArrayLiteral(List<Term.Raw> elements)
    {
        this.elements = elements;
    }

    private Term.Raw forReceiver(ColumnSpecification receiver)
    {
        AbstractType<?> type = receiver.type.unwrap();
        if (type instanceof VectorType)
            return new Vectors.Literal(elements);
        if (type instanceof ListType)
            return new Lists.Literal(elements);
        throw new InvalidRequestException(String.format("Unexpected receiver type '%s'; only list and vector are expected", type.asCQL3Type()));
    }

    @Override
    public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
    {
        return forReceiver(receiver).prepare(keyspace, receiver);
    }

    @Override
    public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
    {
        return forReceiver(receiver).testAssignment(keyspace, receiver);
    }

    @Override
    public AbstractType<?> getExactTypeIfKnown(String keyspace)
    {
        // without knowing the receiver, can't say if this is a ListType or a VectorType
        return null;
    }

    @Override
    public String getText()
    {
        return Lists.listToString(elements, Term.Raw::getText);
    }
}
