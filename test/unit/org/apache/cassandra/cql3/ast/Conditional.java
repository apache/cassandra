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
import java.util.Arrays;
import java.util.List;

import accord.utils.Invariants;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;

public interface Conditional extends Expression
{
    @Override
    default AbstractType<?> type()
    {
        return BooleanType.instance;
    }

    class In implements Conditional
    {
        public final ReferenceExpression symbol;
        public final List<Expression> expressions;

        public In(ReferenceExpression symbol, List<Expression> expressions)
        {
            Invariants.checkArgument(!expressions.isEmpty());
            this.symbol = symbol;
            this.expressions = expressions;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            symbol.toCQL(sb, indent);
            sb.append(" IN ");
            sb.append('(');
            for (Expression e : expressions)
            {
                e.toCQL(sb, indent);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2); // ", "
            sb.append(')');
        }
    }

    class Builder
    {
        private final List<Conditional> sub = new ArrayList<>();

        public boolean isEmpty()
        {
            return sub.isEmpty();
        }

        private Builder add(Conditional conditional)
        {
            sub.add(conditional);
            return this;
        }

        public Builder where(Where.Inequalities kind, ReferenceExpression ref, Expression expression)
        {
            return add(Where.create(kind, ref, expression));
        }

        public Builder in(ReferenceExpression symbol, Expression... expressions)
        {
            return add(new In(symbol, Arrays.asList(expressions)));
        }

        public Builder in(ReferenceExpression symbol, List<Expression> expressions)
        {
            return add(new In(symbol, expressions));
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
