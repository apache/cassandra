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

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.jboss.byteman.rule.helper.Helper;

import static org.apache.cassandra.inject.Expression.THIS;
import static org.apache.cassandra.inject.Expression.expr;
import static org.apache.cassandra.inject.Expression.quote;

/**
 * Refer to <a href="https://github.com/bytemanproject/byteman/blob/master/docs/asciidoc/src/main/asciidoc/chapters/Byteman-Rule-Language.adoc"/>
 * and injections.md files in the root directory.
 */
public class ActionBuilder
{
    private Class<?> helperClass = Helper.class;
    private final ConditionsBuilder conditionsBuilder = new ConditionsBuilder();
    private final BindingsBuilder bindingsBuilder = new BindingsBuilder();
    private final ActionsBuilder actionsBuilder = new ActionsBuilder();

    public static ActionBuilder newActionBuilder() {
        return new ActionBuilder();
    }

    private ActionBuilder() {}

    public ActionBuilder withHelperClass(Class<?> helperClass)
    {
        this.helperClass = helperClass;
        return this;
    }

    public Class<?> getHelperClass()
    {
        return helperClass;
    }

    public ConditionsBuilder conditions()
    {
        return conditionsBuilder;
    }

    public BindingsBuilder bindings()
    {
        return bindingsBuilder;
    }

    public ActionsBuilder actions()
    {
        return actionsBuilder;
    }

    /**
     * When an invoke point is set on a method in an interface or in some base class and is not overridden in the
     * class where we want the injection to be executed, we need to add some additional condition. This method
     * configures
     * the action to be invoked only in the instance of the specified class.
     */
    public ActionBuilder onlyInClass(Class<?> targetClass)
    {
        return onlyInClass(targetClass.getName());
    }

    /** @see #onlyInClass(Class) */
    public ActionBuilder onlyInClass(String targetClassName)
    {
        bindings().addBinding("thisClassName", "String", expr(THIS).method("getClass").args().method("getName").args().toString());
        conditions().when(expr("thisClassName").method("equals").args(quote(targetClassName)));
        return this;
    }

    /**
     * the action to be invoked only if it's called from given callerMethodName
     */
    public ActionBuilder callerEquals(String callerMethodName)
    {
        conditions().when(expr("callerEquals").args(quote(callerMethodName)));
        return this;
    }

    private enum LogicOp
    {
        AND, OR, NOT
    }

    public class ConditionsBuilder extends Builder
    {
        private final LinkedList<Object> elements = new LinkedList<>();

        public ConditionsBuilder clear()
        {
            elements.clear();
            return this;
        }

        public ConditionsBuilder when(Object expression)
        {
            if (!elements.isEmpty() && !(elements.getLast() instanceof LogicOp))
            {
                elements.add(LogicOp.AND);
            }
            elements.add(expression);
            return this;
        }

        public ConditionsBuilder not()
        {
            if (!(elements.getLast() instanceof LogicOp))
            {
                elements.add(LogicOp.AND);
            }
            elements.add(LogicOp.NOT);
            return this;
        }

        public ConditionsBuilder and()
        {
            Preconditions.checkState(!(elements.getLast() instanceof LogicOp));
            elements.add(LogicOp.AND);
            return this;
        }

        public ConditionsBuilder or()
        {
            Preconditions.checkState(!(elements.getLast() instanceof LogicOp));
            elements.add(LogicOp.OR);
            return this;
        }

        public ConditionsBuilder trueLiteral()
        {
            return when("TRUE");
        }

        public ConditionsBuilder falseLiteral()
        {
            return when("FALSE");
        }

        public String buildInternal()
        {
            if (elements.isEmpty())
            {
                return "IF TRUE";
            }
            else
            {
                return String.format("IF %s", elements.stream().map(Object::toString).collect(Collectors.joining(" ")));
            }
        }
    }

    public class ActionsBuilder extends Builder
    {
        private final List<String> actions = new LinkedList<>();

        public ActionBuilder doThrow(Object newThrowableExpression)
        {
            actions.add(String.format("throw %s", newThrowableExpression));
            return ActionBuilder.this;
        }

        public ActionBuilder doThrow(Class<? extends Throwable> exceptionClass, Object... args)
        {
            return doThrow(Expression.newInstance(exceptionClass).args(args));
        }

        public ActionBuilder doReturn(Object returnExpression)
        {
            actions.add(String.format("return %s", returnExpression));
            return ActionBuilder.this;
        }

        public ActionsBuilder doAction(Object expression)
        {
            actions.add(expression.toString());
            return this;
        }

        public String buildInternal()
        {
            if (actions.isEmpty())
            {
                return "DO NOTHING";
            }
            else
            {
                return String.format("DO %s", String.join(";\n", actions));
            }
        }
    }

    public class BindingsBuilder extends Builder
    {
        private final List<Binding> bindings = new LinkedList<>();

        public BindingsBuilder addBinding(String name, String type, String expression)
        {
            bindings.add(new Binding(name, type, expression));
            return this;
        }

        public BindingsBuilder clear()
        {
            bindings.clear();
            return this;
        }

        public class Binding
        {
            public final String name;
            public final String type;
            public final String expression;

            public Binding(String name, String type, String expression)
            {
                this.name = name;
                this.type = type;
                this.expression = expression;
            }

            @Override
            public String toString()
            {
                return String.format("%s:%s = %s", name, type, expression);
            }
        }

        @Override
        String buildInternal()
        {
            if (bindings.isEmpty())
            {
                return "";
            }
            else
            {
                return String.format("BIND %s", bindings.stream().map(Binding::toString).collect(Collectors.joining("\n")));
            }
        }
    }

    String buildInternal()
    {
        return String.format("%s\n%s\n%s", bindings().buildInternal(), conditions().buildInternal(), actions().buildInternal());
    }

    public abstract class Builder
    {
        abstract String buildInternal();

        public ActionsBuilder actions()
        {
            return actionsBuilder;
        }

        public ConditionsBuilder conditions()
        {
            return conditionsBuilder;
        }

        public BindingsBuilder bindings()
        {
            return bindingsBuilder;
        }

        public ActionBuilder toActionBuilder()
        {
            return ActionBuilder.this;
        }
    }
}
