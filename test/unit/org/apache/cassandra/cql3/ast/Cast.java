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

public class Cast implements Expression
{
    private final Expression e;
    private final AbstractType<?> type;

    public Cast(Expression e, AbstractType<?> type)
    {
        this.e = e;
        this.type = type;
    }

    @Override
    public void toCQL(StringBuilder sb, CQLFormatter formatter)
    {
        sb.append("CAST (");
        e.toCQL(sb, formatter);
        sb.append(" AS ");
        sb.append(type.asCQL3Type());
        sb.append(")");
    }

    @Override
    public Stream<? extends Element> stream()
    {
        return Stream.of(e);
    }

    @Override
    public AbstractType<?> type()
    {
        return type;
    }

    @Override
    public Expression visit(Visitor v)
    {
        var u = v.visit(this);
        if (u != this) return u;
        Expression e = this.e.visit(v);
        if (e == this.e) return this;
        return new Cast(e, type);
    }
}
