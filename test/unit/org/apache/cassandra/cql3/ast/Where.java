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

public class Where implements Conditional
{
    public enum Inequalities
    {
        EQUAL("="),
        NOT_EQUAL("!="),
        GREATER_THAN(">"),
        GREATER_THAN_EQ(">="),
        LESS_THAN("<"),
        LESS_THAN_EQ("<=");

        private final String value;

        Inequalities(String value)
        {
            this.value = value;
        }
    }

    public final Inequalities kind;
    public final ReferenceExpression symbol;
    public final Expression expression;

    private Where(Inequalities kind, ReferenceExpression symbol, Expression expression)
    {
        this.kind = kind;
        this.symbol = symbol;
        this.expression = expression;
    }

    public static Where create(Inequalities kind, ReferenceExpression ref, Expression expression)
    {
        return new Where(kind, ref instanceof Symbol ? Reference.of(ref) : ref, expression);
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        symbol.toCQL(sb, indent);
        sb.append(' ').append(kind.value).append(' ');
        expression.toCQL(sb, indent);
    }

    @Override
    public Stream<? extends Element> stream()
    {
        return Stream.of(symbol, expression);
    }
}
