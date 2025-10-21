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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;

public class Reference implements ReferenceExpression
{
    public final List<ReferenceExpression> path;

    private Reference(List<ReferenceExpression> path)
    {
        if (path.isEmpty())
            throw new IllegalArgumentException("Reference may not be empty");
        this.path = path;
        if (path.stream().anyMatch(e -> e.toCQL().contains("\"\"")))
            new Throwable().printStackTrace();
    }

    public static Reference of(ReferenceExpression top)
    {
        return new Reference(Collections.singletonList(Objects.requireNonNull(top)));
    }

    public static Reference of(ReferenceExpression top, ReferenceExpression next)
    {
        return new Reference(List.of(top, next));
    }

    @Override
    public AbstractType<?> type()
    {
        return path.get(path.size() - 1).type();
    }

    public Builder unbuild()
    {
        return new Builder(path);
    }

    public Reference add(String symbol, AbstractType<?> subType)
    {
        return unbuild().add(symbol, subType).build();
    }

    public Reference add(Expression expression)
    {
        if (expression instanceof Reference)
        {
            Reference other = (Reference) expression;
            List<ReferenceExpression> path = new ArrayList<>(this.path.size() + other.path.size());
            path.addAll(this.path);
            path.addAll(other.path);
            return new Reference(path);
        }
        else if (expression instanceof Symbol)
        {
            return unbuild().add((Symbol) expression).build();
        }
        else
        {
            return unbuild().add(expression.name(), expression.type()).build();
        }
    }

    public Reference lastAsCollection(Function<ReferenceExpression, CollectionAccess> fn)
    {
        List<ReferenceExpression> path = new ArrayList<>(this.path.size());
        for (int i = 0; i < this.path.size() - 1; i++)
            path.add(this.path.get(i));
        ReferenceExpression last = this.path.get(this.path.size() - 1);
        path.add(Objects.requireNonNull(fn.apply(last)));
        return new Reference(path);
    }

    @Override
    public void toCQL(StringBuilder sb, CQLFormatter formatter)
    {
        path.forEach(p -> {
            p.toCQL(sb, formatter);
            sb.append('.');
        });
        sb.setLength(sb.length() - 1); // last .
    }

    @Override
    public Stream<? extends Element> stream()
    {
        return path.stream();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Reference elements = (Reference) o;
        return Objects.equals(path, elements.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path);
    }

    @Override
    public String toString()
    {
        return toCQL();
    }

    public static class Builder
    {
        private final List<ReferenceExpression> path;

        public Builder()
        {
            path = new ArrayList<>();
        }

        private Builder(List<ReferenceExpression> path)
        {
            this.path = new ArrayList<>(path.size() + 1);
            this.path.addAll(path);
        }

        public Builder add(String name)
        {
            return add(new Symbol(name, BytesType.instance));
        }

        public Builder add(String symbol, AbstractType<?> subType)
        {
            return add(new Symbol(symbol, subType));
        }

        public Builder add(Symbol symbol)
        {
            path.add(Objects.requireNonNull(symbol));
            return this;
        }

        public Builder add(String[] paths)
        {
            for (String name : paths)
                path.add(new Symbol(name, BytesType.instance));
            return this;
        }

        public Reference build()
        {
            switch (path.size())
            {
                case 0: throw new IllegalStateException("Unable to make a Reference from an empty path");
                case 1: return Reference.of(path.get(0));
                default: return new Reference(ImmutableList.copyOf(path));
            }
        }
    }
}
