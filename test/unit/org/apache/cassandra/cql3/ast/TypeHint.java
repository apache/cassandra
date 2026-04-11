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

import java.util.stream.Stream;

import org.apache.cassandra.db.marshal.AbstractType;

public class TypeHint implements Expression
{
    public final Expression e;
    public final AbstractType<?> type;

    public TypeHint(Expression e, AbstractType<?> type)
    {
        this.e = e;
        this.type = type;
    }

    public TypeHint(Expression e)
    {
        this.e = e;
        this.type = e.type();
    }

    /**
     * {@code ? + ?} is not clear to parsing, so rather than assume the type (like we do for literals) we fail and ask
     * the user to CAST... so need to {@link TypeHint} when a {@link Bind} is found.
     *
     * Wait, {@link TypeHint} and not {@link Cast}?  See CASSANDRA-17915...
     */
    public static Expression maybeApplyTypeHint(Expression e)
    {
        if (!(e instanceof Bind))
            return e;
        // see https://the-asf.slack.com/archives/CK23JSY2K/p1663788235000449
        return new TypeHint(e);
    }

    @Override
    public void toCQL(StringBuilder sb, CQLFormatter formatter)
    {
        sb.append('(').append(type.asCQL3Type()).append(") ");
        e.toCQL(sb, formatter);
    }

    @Override
    public AbstractType<?> type()
    {
        return type;
    }

    @Override
    public Stream<? extends Element> stream()
    {
        return Stream.of(e);
    }

    @Override
    public Expression visit(Visitor v)
    {
        var u = v.visit(this);
        if (u != this) return u;
        var e = this.e.visit(v);
        if (e == this.e) return this;
        return new TypeHint(e, type);
    }
}
