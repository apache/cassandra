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

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;

public class FunctionCall implements Expression
{
    private final String name;
    private final List<Expression> arguments;
    private final AbstractType<?> returnType;

    public FunctionCall(String name, List<Expression> arguments, AbstractType<?> returnType)
    {
        this.name = name;
        this.arguments = arguments;
        this.returnType = returnType;
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        sb.append(name).append('(');
        for (Expression e : arguments)
        {
            e.toCQL(sb, indent);
            sb.append(", ");
        }
        if (!arguments.isEmpty())
            sb.setLength(sb.length() - 2);
        sb.append(')');
    }

    @Override
    public Stream<? extends Element> stream()
    {
        return arguments.stream();
    }

    @Override
    public AbstractType<?> type()
    {
        return returnType;
    }

    @Override
    public String name()
    {
        return name;
    }

    public static FunctionCall writetime(String column, AbstractType<?> type)
    {
        return new FunctionCall("writetime", Collections.singletonList(Reference.of(new Symbol(column, type))), LongType.instance);
    }

    public static FunctionCall countStar()
    {
        return new FunctionCall("count(*)", Collections.emptyList(), LongType.instance);
    }

    public static FunctionCall count(String symbol)
    {
        return count(Symbol.unknownType(symbol));
    }

    public static FunctionCall count(Symbol symbol)
    {
        return new FunctionCall("count", Collections.singletonList(Reference.of(symbol)), LongType.instance);
    }
}
