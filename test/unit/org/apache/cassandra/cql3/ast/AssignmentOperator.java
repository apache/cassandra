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

package org.apache.cassandra.cql3.ast;

import java.util.EnumSet;
import java.util.stream.Stream;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;

//TODO this is hacky as Mutation takes Expression then figures out where to put it...
// this can only be applied to UPDATE ... SET
public class AssignmentOperator implements Expression
{
    public enum Kind
    {
        ADD('+'),
        SUBTRACT('-');

        private final char value;

        Kind(char value)
        {
            this.value = value;
        }
    }

    public final Kind kind;
    public final Expression right;

    public AssignmentOperator(Kind kind, Expression right)
    {
        this.kind = kind;
        this.right = right;
    }

    public static EnumSet<Kind> supportsOperators(AbstractType<?> type)
    {
        type = type.unwrap();
        EnumSet<Kind> result = EnumSet.noneOf(Kind.class);
        for (Operator.Kind supported : Operator.supportsOperators(type))
        {
            Kind kind = toKind(supported);
            if (kind != null)
                result.add(kind);
        }
        if (result.isEmpty())
        {
            if (type instanceof CollectionType && type.isMultiCell())
            {
                if (type instanceof SetType || type instanceof ListType)
                    return EnumSet.of(Kind.ADD, Kind.SUBTRACT);
                if (type instanceof MapType)
                {
                    // map supports subtract, but not map - map; only map - set!
                    // since this is annoying to support, for now dropping -
                    return EnumSet.of(Kind.ADD);
                }
                throw new AssertionError("Unexpected collection type: " + type);
            }
        }
        return result;
    }

    private static Kind toKind(Operator.Kind kind)
    {
        switch (kind)
        {
            case ADD: return Kind.ADD;
            case SUBTRACT: return Kind.SUBTRACT;
            default: return null;
        }
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        sb.append(' ').append(kind.value).append("= ");
        right.toCQL(sb, indent);
    }

    @Override
    public Stream<? extends Element> stream()
    {
        return Stream.of(right);
    }

    @Override
    public AbstractType<?> type()
    {
        return right.type();
    }
}
