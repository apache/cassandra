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
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LongType;

public class FunctionCall implements Expression
{
    public final String name;
    public final List<? extends Expression> arguments;
    public final AbstractType<?> returnType;

    public FunctionCall(String name, List<? extends Expression> arguments, AbstractType<?> returnType)
    {
        this.name = name;
        this.arguments = arguments;
        this.returnType = returnType;
    }

    @Override
    public void toCQL(StringBuilder sb, CQLFormatter formatter)
    {
        sb.append(name).append('(');
        for (Expression e : arguments)
        {
            e.toCQL(sb, formatter);
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

    @Override
    public Expression visit(Visitor v)
    {
        var u = v.visit(this);
        if (u != this) return u;
        boolean updated = false;
        List<Expression> as = new ArrayList<>(arguments.size());
        for (Expression e : arguments)
        {
            u = e.visit(v);
            if (u != e)
                updated = true;
            as.add(u);
        }
        if (!updated) return this;
        return new FunctionCall(name, as, returnType);
    }

    public static FunctionCall writetime(Symbol symbol)
    {
        return new FunctionCall("writetime", Collections.singletonList(symbol), LongType.instance);
    }

    public static FunctionCall writetime(String column, AbstractType<?> type)
    {
        return writetime(new Symbol(column, type));
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
        return new FunctionCall("count", Collections.singletonList(symbol), LongType.instance);
    }

    public static FunctionCall tokenByColumns(Symbol... columns)
    {
        return tokenByColumns(Arrays.asList(columns));
    }

    public static FunctionCall tokenByColumns(List<Symbol> columns)
    {
        return new FunctionCall("token", columns, BytesType.instance);
    }

    public static FunctionCall tokenByValue(Value... values)
    {
        return tokenByValue(Arrays.asList(values));
    }

    public static FunctionCall tokenByValue(List<? extends Value> values)
    {
        return new FunctionCall("token", values, BytesType.instance);
    }
}
