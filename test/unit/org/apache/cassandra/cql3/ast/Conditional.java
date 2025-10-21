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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.TableMetadata;

public interface Conditional extends Expression
{
    @Override
    default AbstractType<?> type()
    {
        return BooleanType.instance;
    }

    @Override
    default Conditional visit(Visitor v)
    {
        return v.visit(this);
    }

    default String debugCQL()
    {
        return visit(StandardVisitors.DEBUG).toCQL();
    }

    default List<Conditional> simplify()
    {
        return Collections.singletonList(this);
    }

    static Builder builder()
    {
        return new Builder();
    }

    class Where implements Conditional
    {
        public enum Inequality
        {
            EQUAL("="),
            NOT_EQUAL("!="),
            GREATER_THAN(">"),
            GREATER_THAN_EQ(">="),
            LESS_THAN("<"),
            LESS_THAN_EQ("<=");

            public final String value;

            Inequality(String value)
            {
                this.value = value;
            }

            public boolean test(AbstractType<?> type, ByteBuffer a, ByteBuffer b)
            {
                int rc = type.compare(a, b);
                switch (this)
                {
                    case EQUAL: return rc == 0;
                    case NOT_EQUAL: return rc != 0;
                    case GREATER_THAN: return rc > 0;
                    case GREATER_THAN_EQ: return rc >= 0;
                    case LESS_THAN: return rc < 0;
                    case LESS_THAN_EQ: return rc <=0;
                    default: throw new UnsupportedOperationException(this.name());
                }
            }
        }

        public final Inequality kind;
        public final Expression lhs;
        public final Expression rhs;

        private Where(Inequality kind, Expression lhs, Expression rhs)
        {
            this.kind = kind;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        public static Where create(Inequality kind, Expression ref, Expression expression)
        {
            return new Where(kind, ref, expression);
        }

        @Override
        public String toString()
        {
            return toCQL();
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            lhs.toCQL(sb, formatter);
            sb.append(' ').append(kind.value).append(' ');
            rhs.toCQL(sb, formatter);
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(lhs, rhs);
        }

        @Override
        public Conditional visit(Visitor v)
        {
            var u = v.visit(this);
            if (u != this) return u;
            var lhs = this.lhs.visit(v);
            var rhs = this.rhs.visit(v);
            if (lhs == this.lhs && rhs == this.rhs) return this;
            return new Where(kind, lhs, rhs);
        }
    }

    class Between implements Conditional
    {
        public final Expression ref, start, end;

        public Between(Expression ref, Expression start, Expression end)
        {
            this.ref = ref;
            this.start = start;
            this.end = end;
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            ref.toCQL(sb, formatter);
            sb.append(" BETWEEN ");
            start.toCQL(sb, formatter);
            sb.append(" AND ");
            end.toCQL(sb, formatter);
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(ref, start, end);
        }

        @Override
        public Conditional visit(Visitor v)
        {
            var u = v.visit(this);
            if (u != this) return u;
            Expression ref = this.ref.visit(v);
            Expression start = this.start.visit(v);
            Expression end = this.end.visit(v);
            if (ref == this.ref && start == this.start && end == this.end) return this;
            return new Between(ref, start, end);
        }
    }

    class In implements Conditional
    {
        public final ReferenceExpression ref;
        public final List<? extends Expression> expressions;

        public In(ReferenceExpression ref, List<? extends Expression> expressions)
        {
            Preconditions.checkArgument(!expressions.isEmpty());
            this.ref = ref;
            this.expressions = expressions;
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            ref.toCQL(sb, formatter);
            sb.append(" IN ");
            sb.append('(');
            for (Expression e : expressions)
            {
                e.toCQL(sb, formatter);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2); // ", "
            sb.append(')');
        }

        @Override
        public Stream<? extends Element> stream()
        {
            List<Element> es = new ArrayList<>(expressions.size() + 1);
            es.add(ref);
            es.addAll(expressions);
            return es.stream();
        }

        @Override
        public Conditional visit(Visitor v)
        {
            var u = v.visit(this);
            if (u != this) return u;
            var symbol = this.ref.visit(v);
            boolean updated = symbol == this.ref;
            List<Expression> expressions = new ArrayList<>(this.expressions.size());
            for (Expression e : this.expressions)
            {
                var e2 = e.visit(v);
                updated |= e == e2;
                expressions.add(e2);
            }
            if (!updated) return this;
            return new In(symbol, expressions);
        }
    }

    class Is implements Conditional
    {
        public enum Kind
        {
            Null("NULL"),
            NotNull("NOT NULL");

            private final String cql;

            Kind(String s)
            {
                this.cql = s;
            }
        }

        public final ReferenceExpression reference;
        public final Kind kind;

        public Is(String symbol, Kind kind)
        {
            this(Symbol.unknownType(symbol), kind);
        }

        public Is(ReferenceExpression reference, Kind kind)
        {
            this.reference = reference;
            this.kind = kind;
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            reference.toCQL(sb, formatter);
            sb.append(" IS ").append(kind.cql);
        }
    }

    class And implements Conditional
    {
        public final Conditional left, right;

        public And(Conditional left, Conditional right)
        {
            this.left = left;
            this.right = right;
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            left.toCQL(sb, formatter);
            sb.append(" AND ");
            right.toCQL(sb, formatter);
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(left, right);
        }

        @Override
        public Conditional visit(Visitor v)
        {
            var u = v.visit(this);
            if (u != this) return u;
            Conditional left = this.left.visit(v);
            Conditional right = this.right.visit(v);
            if (this.left == left && this.right == right) return this;
            return new And(left, right);
        }

        @Override
        public List<Conditional> simplify()
        {
            List<Conditional> result = new ArrayList<>();
            result.addAll(left.simplify());
            result.addAll(right.simplify());
            return result;
        }
    }

    interface EqBuilder<T extends EqBuilder<T>>
    {
        T value(Symbol symbol, Expression e);
        default T value(Symbol symbol, Object value)
        {
            return value(symbol, new Bind(value, symbol.type()));
        }

        default T value(String symbol, int e)
        {
            return value(new Symbol(symbol, Int32Type.instance), Bind.of(e));
        }
    }

    interface EqBuilderPlus<T extends EqBuilderPlus<T>> extends EqBuilder<T>
    {
        TableMetadata metadata();

        default T value(String name, String value)
        {
            Symbol symbol = new Symbol(metadata().getColumn(new ColumnIdentifier(name, true)));
            return value(symbol, new Bind(symbol.type().asCQL3Type().fromCQLLiteral(value), symbol.type()));
        }

        default T value(String name, Object value)
        {
            Symbol symbol = new Symbol(metadata().getColumn(new ColumnIdentifier(name, true)));
            return value(symbol, new Bind(value, symbol.type()));
        }
    }

    interface ConditionalBuilder<T extends ConditionalBuilder<T>> extends EqBuilder<T>
    {

        T where(Expression ref, Where.Inequality kind, Expression expression);
        default T where(Expression ref, Where.Inequality kind, Object value)
        {
            return where(ref, kind, new Bind(value, ref.type()));
        }

        default <Type> T where(String name, Where.Inequality kind, Type value, AbstractType<Type> type)
        {
            return where(new Symbol(name, type), kind, new Bind(value, type));
        }

        default T where(String name, Where.Inequality kind, int value)
        {
            return where(name, kind, value, Int32Type.instance);
        }

        T between(Expression ref, Expression start, Expression end);

        default T between(String name, Expression start, Expression end)
        {
            return between(new Symbol(name, start.type()), start, end);
        }

        T in(ReferenceExpression ref, List<? extends Expression> expressions);

        default T in(ReferenceExpression ref, Expression... expressions)
        {
            return in(ref, Arrays.asList(expressions));
        }

        default T in(String name, Expression... expressions)
        {
            if (expressions == null || expressions.length == 0)
                throw new IllegalArgumentException("expressions may not be empty");
            return in(new Symbol(name, expressions[0].type()), expressions);
        }

        default T in(String name, int... values)
        {
            return in(name, Int32Type.instance, IntStream.of(values).boxed().collect(Collectors.toList()));
        }

        default <Type> T in(String name, AbstractType<Type> type, Type... values)
        {
            return in(name, type, Arrays.asList(values));
        }

        default <Type> T in(String name, AbstractType<Type> type, List<Type> values)
        {
            return in(new Symbol(name, type), values.stream().map(v -> new Bind(v, type)).collect(Collectors.toList()));
        }

        T is(ReferenceExpression ref, Is.Kind kind);

        @Override
        default T value(Symbol symbol, Expression e)
        {
            return where(symbol, Where.Inequality.EQUAL, e);
        }

        default T value(String symbol, int e)
        {
            return value(symbol, e, Int32Type.instance);
        }

        default T value(String symbol, ByteBuffer e)
        {
            return value(symbol, e, BytesType.instance);
        }

        default <Type> T value(String symbol, Type value, AbstractType<Type> type)
        {
            return value(new Symbol(symbol, type), new Bind(value, type));
        }
    }

    interface ConditionalBuilderPlus<T extends ConditionalBuilderPlus<T>> extends ConditionalBuilder<T>, EqBuilderPlus<T>
    {
        default T where(String name, Where.Inequality kind, String value)
        {
            Symbol symbol = new Symbol(metadata().getColumn(new ColumnIdentifier(name, true)));
            return where(symbol, kind, new Bind(symbol.type().asCQL3Type().fromCQLLiteral(value), symbol.type()));
        }

        default T where(String name, Where.Inequality kind, Object value)
        {
            Symbol symbol = new Symbol(metadata().getColumn(new ColumnIdentifier(name, true)));
            return where(symbol, kind, new Bind(value, symbol.type()));
        }
    }

    class Builder implements ConditionalBuilder<Builder>
    {
        private final List<Conditional> sub = new ArrayList<>();

        public boolean isEmpty()
        {
            return sub.isEmpty();
        }

        public Builder add(Conditional conditional)
        {
            sub.add(conditional);
            return this;
        }

        @Override
        public Builder where(Expression ref, Where.Inequality kind, Expression expression)
        {
            return add(Where.create(kind, ref, expression));
        }

        @Override
        public Builder between(Expression ref, Expression start, Expression end)
        {
            return add(new Between(ref, start, end));
        }

        @Override
        public Builder in(ReferenceExpression symbol, List<? extends Expression> expressions)
        {
            return add(new In(symbol, expressions));
        }

        @Override
        public Builder is(ReferenceExpression ref, Is.Kind kind)
        {
            return add(new Is(ref, kind));
        }

        public Builder is(String ref, Is.Kind kind)
        {
            return is(Symbol.unknownType(ref), kind);
        }

        public Conditional build()
        {
            if (sub.isEmpty())
                throw new IllegalStateException("Unable to build an empty Conditional");
            if (sub.size() == 1)
                return sub.get(0);
            Conditional accum = sub.get(0);
            for (int i = 1; i < sub.size(); i++)
                accum = new And(accum, sub.get(i));
            return accum;
        }
    }
}
