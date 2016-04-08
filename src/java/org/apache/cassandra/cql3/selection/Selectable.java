/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cql3.selection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.text.StrBuilder;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public abstract class Selectable
{
    public abstract Selector.Factory newSelectorFactory(CFMetaData cfm, List<ColumnDefinition> defs);

    protected static int addAndGetIndex(ColumnDefinition def, List<ColumnDefinition> l)
    {
        int idx = l.indexOf(def);
        if (idx < 0)
        {
            idx = l.size();
            l.add(def);
        }
        return idx;
    }

    public static interface Raw
    {
        public Selectable prepare(CFMetaData cfm);

        /**
         * Returns true if any processing is performed on the selected column.
         **/
        public boolean processesSelection();
    }

    public static class WritetimeOrTTL extends Selectable
    {
        public final ColumnIdentifier id;
        public final boolean isWritetime;

        public WritetimeOrTTL(ColumnIdentifier id, boolean isWritetime)
        {
            this.id = id;
            this.isWritetime = isWritetime;
        }

        @Override
        public String toString()
        {
            return (isWritetime ? "writetime" : "ttl") + "(" + id + ")";
        }

        public Selector.Factory newSelectorFactory(CFMetaData cfm,
                                                   List<ColumnDefinition> defs)
        {
            ColumnDefinition def = cfm.getColumnDefinition(id);
            if (def == null)
                throw new InvalidRequestException(String.format("Undefined name %s in selection clause", id));
            if (def.isPrimaryKeyColumn())
                throw new InvalidRequestException(
                        String.format("Cannot use selection function %s on PRIMARY KEY part %s",
                                      isWritetime ? "writeTime" : "ttl",
                                      def.name));
            if (def.type.isCollection())
                throw new InvalidRequestException(String.format("Cannot use selection function %s on collections",
                                                                isWritetime ? "writeTime" : "ttl"));

            return WritetimeOrTTLSelector.newFactory(def, addAndGetIndex(def, defs), isWritetime);
        }

        public static class Raw implements Selectable.Raw
        {
            private final ColumnIdentifier.Raw id;
            private final boolean isWritetime;

            public Raw(ColumnIdentifier.Raw id, boolean isWritetime)
            {
                this.id = id;
                this.isWritetime = isWritetime;
            }

            public WritetimeOrTTL prepare(CFMetaData cfm)
            {
                return new WritetimeOrTTL(id.prepare(cfm), isWritetime);
            }

            public boolean processesSelection()
            {
                return true;
            }
        }
    }

    public static class WithFunction extends Selectable
    {
        public final FunctionName functionName;
        public final List<Selectable> args;

        public WithFunction(FunctionName functionName, List<Selectable> args)
        {
            this.functionName = functionName;
            this.args = args;
        }

        @Override
        public String toString()
        {
            return new StrBuilder().append(functionName)
                                   .append("(")
                                   .appendWithSeparators(args, ", ")
                                   .append(")")
                                   .toString();
        }

        public Selector.Factory newSelectorFactory(CFMetaData cfm, List<ColumnDefinition> defs)
        {
            SelectorFactories factories  =
                    SelectorFactories.createFactoriesAndCollectColumnDefinitions(args, cfm, defs);

            // We need to circumvent the normal function lookup process for toJson() because instances of the function
            // are not pre-declared (because it can accept any type of argument).
            Function fun;
            if (functionName.equalsNativeFunction(ToJsonFct.NAME))
                fun = ToJsonFct.getInstance(factories.getReturnTypes());
            else
                fun = FunctionResolver.get(cfm.ksName, functionName, factories.newInstances(), cfm.ksName, cfm.cfName, null);

            if (fun == null)
                throw new InvalidRequestException(String.format("Unknown function '%s'", functionName));

            if (fun.returnType() == null)
                throw new InvalidRequestException(String.format("Unknown function %s called in selection clause",
                                                                functionName));

            return AbstractFunctionSelector.newFactory(fun, factories);
        }

        public static class Raw implements Selectable.Raw
        {
            private final FunctionName functionName;
            private final List<Selectable.Raw> args;

            public Raw(FunctionName functionName, List<Selectable.Raw> args)
            {
                this.functionName = functionName;
                this.args = args;
            }

            public static Raw newCountRowsFunction()
            {
                return new Raw(AggregateFcts.countRowsFunction.name(),
                               Collections.emptyList());
            }

            public WithFunction prepare(CFMetaData cfm)
            {
                List<Selectable> preparedArgs = new ArrayList<>(args.size());
                for (Selectable.Raw arg : args)
                    preparedArgs.add(arg.prepare(cfm));
                return new WithFunction(functionName, preparedArgs);
            }

            public boolean processesSelection()
            {
                return true;
            }
        }
    }

    public static class WithCast extends Selectable
    {
        private final CQL3Type type;
        private final Selectable arg;

        public WithCast(Selectable arg, CQL3Type type)
        {
            this.arg = arg;
            this.type = type;
        }

        @Override
        public String toString()
        {
            return String.format("cast(%s as %s)", arg, type.toString().toLowerCase());
        }

        public Selector.Factory newSelectorFactory(CFMetaData cfm, List<ColumnDefinition> defs)
        {
            SelectorFactories factories  =
                    SelectorFactories.createFactoriesAndCollectColumnDefinitions(Collections.singletonList(arg), cfm, defs);

            Selector.Factory factory = factories.get(0);

            // If the user is trying to cast a type on its own type we simply ignore it.
            if (type.getType().equals(factory.getReturnType()))
            {
                return factory;
            }

            FunctionName name = FunctionName.nativeFunction(CastFcts.getFunctionName(type));
            Function fun = FunctionResolver.get(cfm.ksName, name, factories.newInstances(), cfm.ksName, cfm.cfName, null);

            if (fun == null)
            {
                    throw new InvalidRequestException(String.format("%s cannot be cast to %s",
                                                                    defs.get(0).name,
                                                                    type));
            }
            return AbstractFunctionSelector.newFactory(fun, factories);
        }

        public static class Raw implements Selectable.Raw
        {
            private final CQL3Type type;
            private final Selectable.Raw arg;

            public Raw(Selectable.Raw arg, CQL3Type type)
            {
                this.arg = arg;
                this.type = type;
            }

            public WithCast prepare(CFMetaData cfm)
            {
                return new WithCast(arg.prepare(cfm), type);
            }

            public boolean processesSelection()
            {
                return true;
            }
        }
    }

    public static class WithFieldSelection extends Selectable
    {
        public final Selectable selected;
        public final ColumnIdentifier field;

        public WithFieldSelection(Selectable selected, ColumnIdentifier field)
        {
            this.selected = selected;
            this.field = field;
        }

        @Override
        public String toString()
        {
            return String.format("%s.%s", selected, field);
        }

        public Selector.Factory newSelectorFactory(CFMetaData cfm, List<ColumnDefinition> defs)
        {
            Selector.Factory factory = selected.newSelectorFactory(cfm, defs);
            AbstractType<?> type = factory.newInstance().getType();
            if (!type.isUDT())
            {
                throw new InvalidRequestException(
                        String.format("Invalid field selection: %s of type %s is not a user type",
                                selected,
                                type.asCQL3Type()));
            }

            UserType ut = (UserType) type;
            int fieldIndex = ((UserType) type).fieldPosition(field);
            if (fieldIndex == -1)
            {
                throw new InvalidRequestException(String.format("%s of type %s has no field %s",
                        selected, type.asCQL3Type(), field));
            }

            return FieldSelector.newFactory(ut, fieldIndex, factory);
        }

        public static class Raw implements Selectable.Raw
        {
            private final Selectable.Raw selected;
            private final ColumnIdentifier.Raw field;

            public Raw(Selectable.Raw selected, ColumnIdentifier.Raw field)
            {
                this.selected = selected;
                this.field = field;
            }

            public WithFieldSelection prepare(CFMetaData cfm)
            {
                return new WithFieldSelection(selected.prepare(cfm), field.prepare(cfm));
            }

            public boolean processesSelection()
            {
                return true;
            }
        }
    }
}
