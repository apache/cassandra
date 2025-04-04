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
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.db.marshal.StringType;
import org.apache.cassandra.db.marshal.TemporalType;

public class Operator implements Expression
{
    public enum Kind
    {
        ADD('+'),
        SUBTRACT('-'),
        MULTIPLY('*');
        //TODO support avoiding 42 / 0 and 42 % 0 as these will fail
//        DIVIDE('/'),
//        MOD('%');

        private final char value;

        Kind(char value)
        {
            this.value = value;
        }
    }

    public final Kind kind;
    public final Expression left;
    public final Expression right;

    public Operator(Kind kind, Expression left, Expression right)
    {
        this.kind = kind;
        this.left = left;
        this.right = right;
        if (!left.type().equals(right.type()))
            throw new IllegalArgumentException("Types do not match: left=" + left.type() + ", right=" + right.type());
    }

    public static EnumSet<Kind> supportsOperators(AbstractType<?> type)
    {
        //TODO (coverage): some operators exist but only for different inputs than (type, type)
        // date and time both support +- duration; aka date + duration = date, time - duration = time (looks like all temporal types are this way)
        type = type.unwrap();
        // see org.apache.cassandra.cql3.functions.OperationFcts.OPERATION
        if (type instanceof NumberType)
            return EnumSet.allOf(Kind.class);
        if (type instanceof TemporalType)
            return EnumSet.noneOf(Kind.class); //TODO (coverage): not true, but requires the RHS to be duration type only
        if (type instanceof StringType)
            return EnumSet.of(Kind.ADD);
        return EnumSet.noneOf(Kind.class);
    }

    @Override
    public AbstractType<?> type()
    {
        return left.type();
    }

    @Override
    public void toCQL(StringBuilder sb, CQLFormatter formatter)
    {
        left.toCQL(sb, formatter);
        sb.append(' ').append(kind.value).append(' ');
        right.toCQL(sb, formatter);
    }

    @Override
    public Stream<? extends Element> stream()
    {
        return Stream.of(left, right);
    }

    @Override
    public Expression visit(Visitor v)
    {
        var u = v.visit(this);
        if (u != this) return u;
        var left = this.left.visit(v);
        var right = this.right.visit(v);
        if (left == this.left && right == this.right) return this;
        return new Operator(kind, left, right);
    }

    @Override
    public String toString()
    {
        return visit(StandardVisitors.DEBUG).toCQL();
    }
}
