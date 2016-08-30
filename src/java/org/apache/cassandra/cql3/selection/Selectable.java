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
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;

public interface Selectable extends AssignmentTestable
{
    public Selector.Factory newSelectorFactory(CFMetaData cfm, AbstractType<?> expectedType, List<ColumnDefinition> defs, VariableSpecifications boundNames);

    /**
     * The type of the {@code Selectable} if it can be infered.
     *
     * @param keyspace the keyspace on which the statement for which this is a
     * {@code Selectable} is on.
     * @return the type of this {@code Selectable} if inferrable, or {@code null}
     * otherwise (for instance, the type isn't inferable for a bind marker. Even for
     * literals, the exact type is not inferrable since they are valid for many
     * different types and so this will return {@code null} too).
     */
    public AbstractType<?> getExactTypeIfKnown(String keyspace);

    // Term.Raw overrides this since some literals can be WEAKLY_ASSIGNABLE
    default public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
    {
        AbstractType<?> type = getExactTypeIfKnown(keyspace);
        return type == null ? TestResult.NOT_ASSIGNABLE : type.testAssignment(keyspace, receiver);
    }

    default int addAndGetIndex(ColumnDefinition def, List<ColumnDefinition> l)
    {
        int idx = l.indexOf(def);
        if (idx < 0)
        {
            idx = l.size();
            l.add(def);
        }
        return idx;
    }

    public static abstract class Raw
    {
        public abstract Selectable prepare(CFMetaData cfm);

        /**
         * Returns true if any processing is performed on the selected column.
         **/
        public boolean processesSelection()
        {
            // ColumnIdentifier is the only case that returns false and override this
            return true;
        }
    }

    public static class WithTerm implements Selectable
    {
        /**
         * The names given to unamed bind markers found in selection. In selection clause, we often don't have a good
         * name for bind markers, typically if you have:
         *   SELECT (int)? FROM foo;
         * there isn't a good name for that marker. So we give the same name to all the markers. Note that we could try
         * to differenciate the names by using some increasing number in the name (so [selection_1], [selection_2], ...)
         * but it's actually not trivial to do in the current code and it's not really more helpful since if users wants
         * to bind by position (which they will have to in this case), they can do so at the driver level directly. And
         * so we don't bother.
         * Note that users should really be using named bind markers if they want to be able to bind by names.
         */
        private static final ColumnIdentifier bindMarkerNameInSelection = new ColumnIdentifier("[selection]", true);

        private final Term.Raw rawTerm;

        public WithTerm(Term.Raw rawTerm)
        {
            this.rawTerm = rawTerm;
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return rawTerm.testAssignment(keyspace, receiver);
        }

        public Selector.Factory newSelectorFactory(CFMetaData cfm, AbstractType<?> expectedType, List<ColumnDefinition> defs, VariableSpecifications boundNames) throws InvalidRequestException
        {
            /*
             * expectedType will be null if we have no constraint on what the type should be. For instance, if this term is a bind marker:
             *   - it will be null if we do "SELECT ? FROM foo"
             *   - it won't be null (and be LongType) if we do "SELECT bigintAsBlob(?) FROM foo" because the function constrain it.
             *
             * In the first case, we have to error out: we need to infer the type of the metadata of a SELECT at preparation time, which we can't
             * here (users will have to do "SELECT (varint)? FROM foo" for instance).
             * But in the 2nd case, we're fine and can use the expectedType to "prepare" the bind marker/collect the bound type.
             *
             * Further, the term might not be a bind marker, in which case we sometimes can default to some most-general type. For instance, in
             *   SELECT 3 FROM foo
             * we'll just default the type to 'varint' as that's the most generic type for the literal '3' (this is mostly for convenience, the query
             * is not terribly useful in practice and use can force the type as for the bind marker case through "SELECT (int)3 FROM foo").
             * But note that not all literals can have such default type. For instance, there is no way to infer the type of a UDT literal in a vacuum,
             * and so we simply error out if we have something like:
             *   SELECT { foo: 'bar' } FROM foo
             *
             * Lastly, note that if the term is a terminal literal, we don't have to check it's compatibility with 'expectedType' as any incompatibility
             * would have been found at preparation time.
             */
            AbstractType<?> type = getExactTypeIfKnown(cfm.ksName);
            if (type == null)
            {
                type = expectedType;
                if (type == null)
                    throw new InvalidRequestException("Cannot infer type for term " + this + " in selection clause (try using a cast to force a type)");
            }

            // The fact we default the name to "[selection]" inconditionally means that any bind marker in a
            // selection will have this name. Which isn't terribly helpful, but it's unclear how to provide
            // something a lot more helpful and in practice user can bind those markers by position or, even better,
            // use bind markers.
            Term term = rawTerm.prepare(cfm.ksName, new ColumnSpecification(cfm.ksName, cfm.cfName, bindMarkerNameInSelection, type));
            term.collectMarkerSpecification(boundNames);
            return TermSelector.newFactory(rawTerm.getText(), term, type);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return rawTerm.getExactTypeIfKnown(keyspace);
        }

        @Override
        public String toString()
        {
            return rawTerm.toString();
        }

        public static class Raw extends Selectable.Raw
        {
            private final Term.Raw term;

            public Raw(Term.Raw term)
            {
                this.term = term;
            }

            public Selectable prepare(CFMetaData cfm)
            {
                return new WithTerm(term);
            }
        }
    }

    public static class WritetimeOrTTL implements Selectable
    {
        public final ColumnDefinition column;
        public final boolean isWritetime;

        public WritetimeOrTTL(ColumnDefinition column, boolean isWritetime)
        {
            this.column = column;
            this.isWritetime = isWritetime;
        }

        @Override
        public String toString()
        {
            return (isWritetime ? "writetime" : "ttl") + "(" + column.name + ")";
        }

        public Selector.Factory newSelectorFactory(CFMetaData cfm,
                                                   AbstractType<?> expectedType,
                                                   List<ColumnDefinition> defs,
                                                   VariableSpecifications boundNames)
        {
            if (column.isPrimaryKeyColumn())
                throw new InvalidRequestException(
                        String.format("Cannot use selection function %s on PRIMARY KEY part %s",
                                      isWritetime ? "writeTime" : "ttl",
                                      column.name));
            if (column.type.isCollection())
                throw new InvalidRequestException(String.format("Cannot use selection function %s on collections",
                                                                isWritetime ? "writeTime" : "ttl"));

            return WritetimeOrTTLSelector.newFactory(column, addAndGetIndex(column, defs), isWritetime);
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return isWritetime ? LongType.instance : Int32Type.instance;
        }

        public static class Raw extends Selectable.Raw
        {
            private final ColumnDefinition.Raw id;
            private final boolean isWritetime;

            public Raw(ColumnDefinition.Raw id, boolean isWritetime)
            {
                this.id = id;
                this.isWritetime = isWritetime;
            }

            public WritetimeOrTTL prepare(CFMetaData cfm)
            {
                return new WritetimeOrTTL(id.prepare(cfm), isWritetime);
            }
        }
    }

    public static class WithFunction implements Selectable
    {
        public final Function function;
        public final List<Selectable> args;

        public WithFunction(Function function, List<Selectable> args)
        {
            this.function = function;
            this.args = args;
        }

        @Override
        public String toString()
        {
            return new StrBuilder().append(function.name())
                                   .append("(")
                                   .appendWithSeparators(args, ", ")
                                   .append(")")
                                   .toString();
        }

        public Selector.Factory newSelectorFactory(CFMetaData cfm, AbstractType<?> expectedType, List<ColumnDefinition> defs, VariableSpecifications boundNames)
        {
            SelectorFactories factories = SelectorFactories.createFactoriesAndCollectColumnDefinitions(args, function.argTypes(), cfm, defs, boundNames);
            return AbstractFunctionSelector.newFactory(function, factories);
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return function.returnType();
        }

        public static class Raw extends Selectable.Raw
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

            public Selectable prepare(CFMetaData cfm)
            {
                List<Selectable> preparedArgs = new ArrayList<>(args.size());
                for (Selectable.Raw arg : args)
                    preparedArgs.add(arg.prepare(cfm));

                FunctionName name = functionName;
                // We need to circumvent the normal function lookup process for toJson() because instances of the function
                // are not pre-declared (because it can accept any type of argument). We also have to wait until we have the
                // selector factories of the argument so we can access their final type.
                if (functionName.equalsNativeFunction(ToJsonFct.NAME))
                {
                    return new WithToJSonFunction(preparedArgs);
                }
                // Also, COUNT(x) is equivalent to COUNT(*) for any non-null term x (since count(x) don't care about it's argument outside of check for nullness) and
                // for backward compatibilty we want to support COUNT(1), but we actually have COUNT(x) method for every existing (simple) input types so currently COUNT(1)
                // will throw as ambiguous (since 1 works for any type). So we have have to special case COUNT.
                else if (functionName.equalsNativeFunction(FunctionName.nativeFunction("count"))
                        && preparedArgs.size() == 1
                        && (preparedArgs.get(0) instanceof WithTerm)
                        && (((WithTerm)preparedArgs.get(0)).rawTerm instanceof Constants.Literal))
                {
                    // Note that 'null' isn't a Constants.Literal
                    name = AggregateFcts.countRowsFunction.name();
                    preparedArgs = Collections.emptyList();
                }

                Function fun = FunctionResolver.get(cfm.ksName, name, preparedArgs, cfm.ksName, cfm.cfName, null);

                if (fun == null)
                    throw new InvalidRequestException(String.format("Unknown function '%s'", functionName));

                if (fun.returnType() == null)
                    throw new InvalidRequestException(String.format("Unknown function %s called in selection clause", functionName));

                return new WithFunction(fun, preparedArgs);
            }
        }
    }

    public static class WithToJSonFunction implements Selectable
    {
        public final List<Selectable> args;

        private WithToJSonFunction(List<Selectable> args)
        {
            this.args = args;
        }

        @Override
        public String toString()
        {
            return new StrBuilder().append(ToJsonFct.NAME)
                                   .append("(")
                                   .appendWithSeparators(args, ", ")
                                   .append(")")
                                   .toString();
        }

        public Selector.Factory newSelectorFactory(CFMetaData cfm, AbstractType<?> expectedType, List<ColumnDefinition> defs, VariableSpecifications boundNames)
        {
            SelectorFactories factories = SelectorFactories.createFactoriesAndCollectColumnDefinitions(args, null, cfm, defs, boundNames);
            Function fun = ToJsonFct.getInstance(factories.getReturnTypes());
            return AbstractFunctionSelector.newFactory(fun, factories);
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return UTF8Type.instance;
        }
    }

    public static class WithCast implements Selectable
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

        public Selector.Factory newSelectorFactory(CFMetaData cfm, AbstractType<?> expectedType, List<ColumnDefinition> defs, VariableSpecifications boundNames)
        {
            List<Selectable> args = Collections.singletonList(arg);
            SelectorFactories factories = SelectorFactories.createFactoriesAndCollectColumnDefinitions(args, null, cfm, defs, boundNames);

            Selector.Factory factory = factories.get(0);

            // If the user is trying to cast a type on its own type we simply ignore it.
            if (type.getType().equals(factory.getReturnType()))
                return factory;

            FunctionName name = FunctionName.nativeFunction(CastFcts.getFunctionName(type));
            Function fun = FunctionResolver.get(cfm.ksName, name, args, cfm.ksName, cfm.cfName, null);

            if (fun == null)
            {
                    throw new InvalidRequestException(String.format("%s cannot be cast to %s",
                                                                    defs.get(0).name,
                                                                    type));
            }
            return AbstractFunctionSelector.newFactory(fun, factories);
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return type.getType();
        }

        public static class Raw extends Selectable.Raw
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
        }
    }

    public static class WithFieldSelection implements Selectable
    {
        public final Selectable selected;
        public final FieldIdentifier field;

        public WithFieldSelection(Selectable selected, FieldIdentifier field)
        {
            this.selected = selected;
            this.field = field;
        }

        @Override
        public String toString()
        {
            return String.format("%s.%s", selected, field);
        }

        public Selector.Factory newSelectorFactory(CFMetaData cfm, AbstractType<?> expectedType, List<ColumnDefinition> defs, VariableSpecifications boundNames)
        {
            Selector.Factory factory = selected.newSelectorFactory(cfm, null, defs, boundNames);
            AbstractType<?> type = factory.getColumnSpecification(cfm).type;
            if (!type.isUDT())
            {
                throw new InvalidRequestException(
                        String.format("Invalid field selection: %s of type %s is not a user type",
                                selected,
                                type.asCQL3Type()));
            }

            UserType ut = (UserType) type;
            int fieldIndex = ut.fieldPosition(field);
            if (fieldIndex == -1)
            {
                throw new InvalidRequestException(String.format("%s of type %s has no field %s",
                        selected, type.asCQL3Type(), field));
            }

            return FieldSelector.newFactory(ut, fieldIndex, factory);
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            AbstractType<?> selectedType = selected.getExactTypeIfKnown(keyspace);
            if (selectedType == null || !(selectedType instanceof UserType))
                return null;

            UserType ut = (UserType) selectedType;
            int fieldIndex = ut.fieldPosition(field);
            if (fieldIndex == -1)
                return null;

            return ut.fieldType(fieldIndex);
        }

        public static class Raw extends Selectable.Raw
        {
            private final Selectable.Raw selected;
            private final FieldIdentifier field;

            public Raw(Selectable.Raw selected, FieldIdentifier field)
            {
                this.selected = selected;
                this.field = field;
            }

            public WithFieldSelection prepare(CFMetaData cfm)
            {
                return new WithFieldSelection(selected.prepare(cfm), field);
            }
        }
    }
}
