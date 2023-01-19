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
package org.apache.cassandra.inject;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

public class Expression
{
    private final StringBuilder expression = new StringBuilder();

    public static Expression expr()
    {
        return new Expression();
    }

    public static Expression expr(String str)
    {
        Expression e = new Expression();
        e.expression.append(str);
        return e;
    }

    public static Expression newInstance(Class<?> clazz)
    {
        return expr("new ").append(clazz.getName());
    }

    public static Expression newInstance(String clazz)
    {
        return expr("new ").append(clazz);
    }

    public static Expression clazz(Class<?> clazz)
    {
        return expr(clazz.getName());
    }

    public Expression method(String method)
    {
        if (expression.length() > 0)
        {
            expression.append(".");
        }
        expression.append(method);
        return this;
    }

    public Expression args(Object... args)
    {
        expression.append("(").append(Arrays.stream(args).map(String::valueOf).collect(Collectors.joining(","))).append(")");
        return this;
    }

    public static Expression method(Class<?> clazz, Class<? extends Annotation> annotation)
    {
        List<Method> methods = Arrays.stream(clazz.getDeclaredMethods())
                                     .filter(m -> m.isAnnotationPresent(annotation))
                                     .collect(Collectors.toList());

        Preconditions.checkArgument(methods.size() == 1, "There are " + methods.size() + " methods annotated with " + annotation.getSimpleName());
        return Expression.clazz(clazz).method(methods.get(0).getName());
    }

    public Expression append(String elem)
    {
        expression.append(elem);
        return this;
    }

    @Override
    public String toString()
    {
        return expression.toString();
    }

    public static String quote(String quoted)
    {
        return "\"" + quoted + "\"";
    }

    public static String arg(int n) { return "$" + n; }

    public final static String THIS = "$this";
}
