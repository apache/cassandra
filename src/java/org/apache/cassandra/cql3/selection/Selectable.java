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

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.cql3.selection.Selector.Factory;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.selection.SelectorFactories.createFactoriesAndCollectColumnDefinitions;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public interface Selectable extends AssignmentTestable
{
    public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames);

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

    /**
     * Checks if this {@code Selectable} select columns matching the specified predicate.
     * @return {@code true} if this {@code Selectable} select columns matching the specified predicate,
     * {@code false} otherwise.
     */
    public boolean selectColumns(Predicate<ColumnMetadata> predicate);

    /**
     * Checks if the specified Selectables select columns matching the specified predicate.
     * @param selectables the selectables to check.
     * @return {@code true} if the specified Selectables select columns matching the specified predicate,
      {@code false} otherwise.
     */
    public static boolean selectColumns(List<Selectable> selectables, Predicate<ColumnMetadata> predicate)
    {
        for (Selectable selectable : selectables)
        {
            if (selectable.selectColumns(predicate))
                return true;
        }
        return false;
    }

    /**
     * Checks if any processing is performed on the selected columns, {@code false} otherwise.
     * @return {@code true} if any processing is performed on the selected columns, {@code false} otherwise.
     */
    public default boolean processesSelection()
    {
        // ColumnMetadata is the only case that returns false (if the column is not masked) and overrides this
        return true;
    }

    // Term.Raw overrides this since some literals can be WEAKLY_ASSIGNABLE
    default public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
    {
        AbstractType<?> type = getExactTypeIfKnown(keyspace);
        return type == null ? TestResult.NOT_ASSIGNABLE : type.testAssignment(keyspace, receiver);
    }

    @Override
    public default AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
    {
        return getExactTypeIfKnown(keyspace);
    }

    default int addAndGetIndex(ColumnMetadata def, List<ColumnMetadata> l)
    {
        int idx = l.indexOf(def);
        if (idx < 0)
        {
            idx = l.size();
            l.add(def);
        }
        return idx;
    }

    default ColumnSpecification specForElementOrSlice(Selectable selected, ColumnSpecification receiver, CollectionType.Kind kind, String selectionType)
    {
        switch (kind)
        {
            case LIST: throw new InvalidRequestException(String.format("%s selection is only allowed on sets and maps, but %s is a list", selectionType, selected));
            case SET: return Sets.valueSpecOf(receiver);
            case MAP: return Maps.keySpecOf(receiver);
            default: throw new AssertionError();
        }
    }

    public interface Raw
    {
        public Selectable prepare(TableMetadata table);
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

        public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) throws InvalidRequestException
        {
            /*
             * expectedType will be null if we have no constraint on what the type should be. For instance, if this term is a bind marker:
             *   - it will be null if we do "SELECT ? FROM foo"
             *   - it won't be null (and be LongType) if we do "SELECT bigint_as_blob(?) FROM foo" because the function constrain it.
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
            AbstractType<?> type = getExactTypeIfKnown(table.keyspace);
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
            Term term = rawTerm.prepare(table.keyspace, new ColumnSpecification(table.keyspace, table.name, bindMarkerNameInSelection, type));
            term.collectMarkerSpecification(boundNames);
            return TermSelector.newFactory(rawTerm.getText(), term, type);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return rawTerm.getExactTypeIfKnown(keyspace);
        }

        @Override
        public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
        {
            return rawTerm.getCompatibleTypeIfKnown(keyspace);
        }

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            return false;
        }

        @Override
        public String toString()
        {
            return rawTerm.getText();
        }

        public static class Raw implements Selectable.Raw
        {
            private final Term.Raw term;

            public Raw(Term.Raw term)
            {
                this.term = term;
            }

            public Selectable prepare(TableMetadata table)
            {
                return new WithTerm(term);
            }
        }
    }

    public static class WritetimeOrTTL implements Selectable
    {
        // The order of the variants in the Kind enum matters as they are used in ser/deser
        public enum Kind
        {
            TTL("ttl", Int32Type.instance),
            WRITE_TIME("writetime", LongType.instance),
            MAX_WRITE_TIME("maxwritetime", LongType.instance); // maxwritetime is available after Cassandra 4.1 (exclusive)

            public final String name;
            public final AbstractType<?> returnType;

            public static Kind fromOrdinal(int ordinal)
            {
                return values()[ordinal];
            }

            Kind(String name, AbstractType<?> returnType)
            {
                this.name = name;
                this.returnType = returnType;
            }

            public boolean aggregatesMultiCell()
            {
                return this == MAX_WRITE_TIME;
            }
        }

        public final ColumnMetadata column;
        public final Selectable selectable;
        public final Kind kind;

        public WritetimeOrTTL(ColumnMetadata column, Selectable selectable, Kind kind)
        {
            this.column = column;
            this.selectable = selectable;
            this.kind = kind;
        }

        @Override
        public String toString()
        {
            return kind.name + "(" + selectable + ")";
        }

        public Selector.Factory newSelectorFactory(TableMetadata table,
                                                   AbstractType<?> expectedType,
                                                   List<ColumnMetadata> defs,
                                                   VariableSpecifications boundNames)
        {
            if (column.isPrimaryKeyColumn())
                throw new InvalidRequestException(
                        String.format("Cannot use selection function %s on PRIMARY KEY part %s",
                                      kind.name,
                                      column.name));

            Selector.Factory factory = selectable.newSelectorFactory(table, expectedType, defs, boundNames);
            boolean isMultiCell = factory.getColumnSpecification(table).type.isMultiCell();

            return WritetimeOrTTLSelector.newFactory(factory, addAndGetIndex(column, defs), kind, isMultiCell);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            AbstractType<?> type = kind.returnType;
            return column.type.isMultiCell() && !kind.aggregatesMultiCell() ? ListType.getInstance(type, false) : type;
        }

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            return selectable.selectColumns(predicate);
        }

        public static class Raw implements Selectable.Raw
        {
            private final Selectable.RawIdentifier column;
            private final Selectable.Raw selected;
            private final Kind kind;

            public Raw(Selectable.RawIdentifier column, Selectable.Raw selected, Kind kind)
            {
                this.column = column;
                this.selected = selected;
                this.kind = kind;
            }

            @Override
            public WritetimeOrTTL prepare(TableMetadata table)
            {
                return new WritetimeOrTTL((ColumnMetadata) column.prepare(table), selected.prepare(table), kind);
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
            return function.columnName(args.stream().map(Object::toString).collect(Collectors.toList()));
        }

        public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames)
        {
            SelectorFactories factories = SelectorFactories.createFactoriesAndCollectColumnDefinitions(args, function.argTypes(), table, defs, boundNames);
            return AbstractFunctionSelector.newFactory(function, factories);
        }

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            return Selectable.selectColumns(args, predicate);
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return function.returnType();
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

            public static Raw newOperation(char operator, Selectable.Raw left, Selectable.Raw right)
            {
                return new Raw(OperationFcts.getFunctionNameFromOperator(operator),
                               Arrays.asList(left, right));
            }

            public static Raw newNegation(Selectable.Raw arg)
            {
                return new Raw(FunctionName.nativeFunction(OperationFcts.NEGATION_FUNCTION_NAME),
                               Collections.singletonList(arg));
            }

            @Override
            public Selectable prepare(TableMetadata table)
            {
                List<Selectable> preparedArgs = new ArrayList<>(args.size());
                for (Selectable.Raw arg : args)
                    preparedArgs.add(arg.prepare(table));

                FunctionName name = functionName;
                // COUNT(x) is equivalent to COUNT(*) for any non-null term x (since count(x) don't care about its
                // argument outside of check for nullness) and for backward compatibilty we want to support COUNT(1),
                // but we actually have COUNT(x) method for every existing (simple) input types so currently COUNT(1)
                // will throw as ambiguous (since 1 works for any type). So we have to special case COUNT.
                if (functionName.equalsNativeFunction(FunctionName.nativeFunction("count"))
                        && preparedArgs.size() == 1
                        && (preparedArgs.get(0) instanceof WithTerm)
                        && (((WithTerm)preparedArgs.get(0)).rawTerm instanceof Constants.Literal))
                {
                    // Note that 'null' isn't a Constants.Literal
                    name = AggregateFcts.countRowsFunction.name();
                    preparedArgs = Collections.emptyList();
                }

                Function fun = FunctionResolver.get(table.keyspace, name, preparedArgs, table.keyspace, table.name, null);

                if (fun == null)
                    throw new InvalidRequestException(String.format("Unknown function '%s'", functionName));

                if (fun.returnType() == null)
                    throw new InvalidRequestException(String.format("Unknown function %s called in selection clause", functionName));

                return new WithFunction(fun, preparedArgs);
            }
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

        public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames)
        {
            List<Selectable> args = Collections.singletonList(arg);
            SelectorFactories factories = SelectorFactories.createFactoriesAndCollectColumnDefinitions(args, null, table, defs, boundNames);

            Selector.Factory factory = factories.get(0);

            // If the user is trying to cast a type on its own type we simply ignore it.
            if (type.getType().equals(factory.getReturnType()))
                return factory;

            FunctionName name = FunctionName.nativeFunction(CastFcts.getFunctionName(type));
            Function fun = FunctionResolver.get(table.keyspace, name, args, table.keyspace, table.name, null);

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

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            return arg.selectColumns(predicate);
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

            public WithCast prepare(TableMetadata table)
            {
                return new WithCast(arg.prepare(table), type);
            }
        }
    }

    /**
     * Represents the selection of the field of a UDT (eg. t.f).
     */
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

        public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames)
        {
            AbstractType<?> expectedUdtType = null;

            // If the UDT is between parentheses, we know that it is not a tuple with a single element.
            if (selected instanceof BetweenParenthesesOrWithTuple)
            {
                BetweenParenthesesOrWithTuple betweenParentheses = (BetweenParenthesesOrWithTuple) selected;
                expectedUdtType = betweenParentheses.selectables.get(0).getExactTypeIfKnown(table.keyspace);
            }

            Selector.Factory factory = selected.newSelectorFactory(table, expectedUdtType, defs, boundNames);
            AbstractType<?> type = factory.getReturnType();
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

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            return selected.selectColumns(predicate);
        }

        public static class Raw implements Selectable.Raw
        {
            private final Selectable.Raw selected;
            private final FieldIdentifier field;

            public Raw(Selectable.Raw selected, FieldIdentifier field)
            {
                this.selected = selected;
                this.field = field;
            }

            public WithFieldSelection prepare(TableMetadata table)
            {
                return new WithFieldSelection(selected.prepare(table), field);
            }
        }
    }

    /**
     * {@code Selectable} for {@code Selectable} between parentheses or tuples.
     * <p>The parser cannot differentiate between a single element between parentheses or a single element tuple.
     * By consequence, we are forced to wait until the type is known to be able to differentiate them.</p>
     */
    public static class BetweenParenthesesOrWithTuple implements Selectable
    {
        /**
         * The tuple elements or the element between the parentheses
         */
        private final List<Selectable> selectables;

        public BetweenParenthesesOrWithTuple(List<Selectable> selectables)
        {
            this.selectables = selectables;
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            if (selectables.size() == 1 && !receiver.type.isTuple())
                return selectables.get(0).testAssignment(keyspace, receiver);

            return Tuples.testTupleAssignment(receiver, selectables);
        }

        @Override
        public Factory newSelectorFactory(TableMetadata cfm,
                                          AbstractType<?> expectedType,
                                          List<ColumnMetadata> defs,
                                          VariableSpecifications boundNames)
        {
            AbstractType<?> type = getExactTypeIfKnown(cfm.keyspace);
            if (type == null)
            {
                type = expectedType;
                if (type == null)
                    throw invalidRequest("Cannot infer type for term %s in selection clause (try using a cast to force a type)",
                                         this);
            }

            if (selectables.size() == 1 && !type.isTuple())
                return newBetweenParenthesesSelectorFactory(cfm, expectedType, defs, boundNames);

            return newTupleSelectorFactory(cfm, (TupleType) type, defs, boundNames);
        }

        private Factory newBetweenParenthesesSelectorFactory(TableMetadata cfm,
                                                             AbstractType<?> expectedType,
                                                             List<ColumnMetadata> defs,
                                                             VariableSpecifications boundNames)
        {
            Selectable selectable = selectables.get(0);
            final Factory factory = selectable.newSelectorFactory(cfm, expectedType, defs, boundNames);

            return new ForwardingFactory()
            {
                protected Factory delegate()
                {
                    return factory;
                }

                protected String getColumnName()
                {
                    return String.format("(%s)", factory.getColumnName());
                }
            };
        }

        private Factory newTupleSelectorFactory(TableMetadata cfm,
                                                TupleType tupleType,
                                                List<ColumnMetadata> defs,
                                                VariableSpecifications boundNames)
        {
            SelectorFactories factories = createFactoriesAndCollectColumnDefinitions(selectables,
                                                                                     tupleType.allTypes(),
                                                                                     cfm,
                                                                                     defs,
                                                                                     boundNames);

            return TupleSelector.newFactory(tupleType, factories);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            // If there is only one element we cannot know if it is an element between parentheses or a tuple
            // with only one element. By consequence, we need to force the user to specify the type.
            if (selectables.size() == 1)
                return null;

            return Tuples.getExactTupleTypeIfKnown(selectables, p -> p.getExactTypeIfKnown(keyspace));
        }

        @Override
        public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
        {
            // If there is only one element we cannot know if it is an element between parentheses or a tuple
            // with only one element. By consequence, we need to force the user to specify the type.
            if (selectables.size() == 1)
                return null;

            return Tuples.getExactTupleTypeIfKnown(selectables, p -> p.getCompatibleTypeIfKnown(keyspace));
        }

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            return Selectable.selectColumns(selectables, predicate);
        }

        @Override
        public String toString()
        {
            return Tuples.tupleToString(selectables);
        }

        public static class Raw implements Selectable.Raw
        {
            private final List<Selectable.Raw> raws;

            public Raw(List<Selectable.Raw> raws)
            {
                this.raws = raws;
            }

            @Override
            public Selectable prepare(TableMetadata cfm)
            {
                return new BetweenParenthesesOrWithTuple(raws.stream().map(p -> p.prepare(cfm)).collect(Collectors.toList()));
            }
        }
    }

    /**
     * <code>Selectable</code> for literal Lists.
     */
    public static class WithArrayLiteral implements Selectable
    {
        /**
         * The list elements
         */
        protected final List<Selectable> selectables;

        public WithArrayLiteral(List<Selectable> selectables)
        {
            this.selectables = selectables;
        }

        private Selectable target(AbstractType<?> target)
        {
            // when the target isn't known, fallback to list; cases like "SELECT [1, 2]" can't be known, but used to be list type!
            // If a vector is actually desired, then can use type cast/hints: "SELECT (vector<int, 2>) [k, v1]"
            if (target == null || target instanceof ListType)
                return new WithList(selectables);
            else if (target.isVector())
                return new WithVector(selectables);
            throw new IllegalArgumentException("Unsupported target type: " + target.asCQL3Type());
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return target(receiver == null ? null : receiver.type).testAssignment(keyspace, receiver);
        }

        @Override
        public Factory newSelectorFactory(TableMetadata cfm,
                                          AbstractType<?> expectedType,
                                          List<ColumnMetadata> defs,
                                          VariableSpecifications boundNames)
        {
            return target(expectedType).newSelectorFactory(cfm, expectedType, defs, boundNames);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            // TODO try to pass in a target to this API
            // default to list when type is being infered
            return new WithList(selectables).getExactTypeIfKnown(keyspace);
        }

        @Override
        public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
        {
            // TODO try to pass in a target to this API
            // default to list when type is being infered
            return new WithList(selectables).getCompatibleTypeIfKnown(keyspace);
        }

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            return Selectable.selectColumns(selectables, predicate);
        }

        @Override
        public String toString()
        {
            return Lists.listToString(selectables);
        }

        public int getSize()
        {
            return selectables.size();
        }

        public static class Raw implements Selectable.Raw
        {
            private final List<Selectable.Raw> raws;

            public Raw(List<Selectable.Raw> raws)
            {
                this.raws = raws;
            }

            @Override
            public Selectable prepare(TableMetadata cfm)
            {
                return new WithArrayLiteral(raws.stream().map(p -> p.prepare(cfm)).collect(Collectors.toList()));
            }
        }
    }

    public static class WithList extends WithArrayLiteral
    {
        public WithList(List<Selectable> selectables)
        {
            super(selectables);
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return Lists.testListAssignment(receiver, selectables);
        }

        @Override
        public Factory newSelectorFactory(TableMetadata cfm,
                                          AbstractType<?> expectedType,
                                          List<ColumnMetadata> defs,
                                          VariableSpecifications boundNames)
        {
            AbstractType<?> type = getExactTypeIfKnown(cfm.keyspace);
            if (type == null)
            {
                type = expectedType;
                if (type == null)
                    throw invalidRequest("Cannot infer type for term %s in selection clause (try using a cast to force a type)",
                                         this);
            }

            ListType<?> listType = (ListType<?>) type;

            List<AbstractType<?>> expectedTypes = new ArrayList<>(selectables.size());
            for (int i = 0, m = selectables.size(); i < m; i++)
                expectedTypes.add(listType.getElementsType());

            SelectorFactories factories = createFactoriesAndCollectColumnDefinitions(selectables,
                                                                                     expectedTypes,
                                                                                     cfm,
                                                                                     defs,
                                                                                     boundNames);
            return ListSelector.newFactory(type, factories);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return Lists.getExactListTypeIfKnown(selectables, p -> p.getExactTypeIfKnown(keyspace));
        }

        @Override
        public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
        {
            return Lists.getPreferredCompatibleType(selectables, p -> p.getCompatibleTypeIfKnown(keyspace));
        }

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            return Selectable.selectColumns(selectables, predicate);
        }

        @Override
        public String toString()
        {
            return Lists.listToString(selectables);
        }
    }

    public static class WithVector extends WithArrayLiteral
    {
        public WithVector(List<Selectable> selectables)
        {
            super(selectables);
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return Vectors.testVectorAssignment(receiver, selectables);
        }

        @Override
        public Factory newSelectorFactory(TableMetadata cfm,
                                          AbstractType<?> expectedType,
                                          List<ColumnMetadata> defs,
                                          VariableSpecifications boundNames)
        {
            AbstractType<?> type = getExactTypeIfKnown(cfm.keyspace);
            if (type == null)
            {
                type = expectedType;
                if (type == null)
                    throw invalidRequest("Cannot infer type for term %s in selection clause (try using a cast to force a type)",
                                         this);
            }

            VectorType<?> vectorType = (VectorType<?>) type;
            if (vectorType.dimension != selectables.size())
                throw invalidRequest("Unable to create a vector selector of type %s from %d elements", vectorType.asCQL3Type(), selectables.size());

            List<AbstractType<?>> expectedTypes = new ArrayList<>(selectables.size());
            for (int i = 0, m = selectables.size(); i < m; i++)
                expectedTypes.add(vectorType.getElementsType());

            SelectorFactories factories = createFactoriesAndCollectColumnDefinitions(selectables,
                                                                                     expectedTypes,
                                                                                     cfm,
                                                                                     defs,
                                                                                     boundNames);
            return VectorSelector.newFactory(type, factories);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return Vectors.getExactVectorTypeIfKnown(selectables, p -> p.getExactTypeIfKnown(keyspace));
        }

        @Override
        public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
        {
            return Vectors.getPreferredCompatibleType(selectables, p -> p.getCompatibleTypeIfKnown(keyspace));
        }

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            return Selectable.selectColumns(selectables, predicate);
        }

        @Override
        public String toString()
        {
            return Lists.listToString(selectables);
        }
    }

    /**
     * <code>Selectable</code> for literal Sets.
     */
    public static class WithSet implements Selectable
    {
        /**
         * The set elements
         */
        private final List<Selectable> selectables;

        public WithSet(List<Selectable> selectables)
        {
            this.selectables = selectables;
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return Sets.testSetAssignment(receiver, selectables);
        }

        @Override
        public Factory newSelectorFactory(TableMetadata cfm,
                                          AbstractType<?> expectedType,
                                          List<ColumnMetadata> defs,
                                          VariableSpecifications boundNames)
        {
            AbstractType<?> type = getExactTypeIfKnown(cfm.keyspace);
            if (type == null)
            {
                type = expectedType;
                if (type == null)
                    throw invalidRequest("Cannot infer type for term %s in selection clause (try using a cast to force a type)",
                                         this);
            }

            // The parser treats empty Maps as Sets so if the type is a MapType we know that the Map is empty
            if (type instanceof MapType)
                return MapSelector.newFactory(type, Collections.emptyList());

            SetType<?> setType = (SetType<?>) type;

            if (setType.getElementsType() == DurationType.instance)
                throw invalidRequest("Durations are not allowed inside sets: %s", setType.asCQL3Type());

            List<AbstractType<?>> expectedTypes = new ArrayList<>(selectables.size());
            for (int i = 0, m = selectables.size(); i < m; i++)
                expectedTypes.add(setType.getElementsType());

            SelectorFactories factories = createFactoriesAndCollectColumnDefinitions(selectables,
                                                                                     expectedTypes,
                                                                                     cfm,
                                                                                     defs,
                                                                                     boundNames);

            return SetSelector.newFactory(type, factories);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return Sets.getExactSetTypeIfKnown(selectables, p -> p.getExactTypeIfKnown(keyspace));
        }

        @Override
        public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
        {
            return Sets.getPreferredCompatibleType(selectables, p -> p.getCompatibleTypeIfKnown(keyspace));
        }

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            return Selectable.selectColumns(selectables, predicate);
        }

        @Override
        public String toString()
        {
            return Sets.setToString(selectables);
        }

        public static class Raw implements Selectable.Raw
        {
            private final List<Selectable.Raw> raws;

            public Raw(List<Selectable.Raw> raws)
            {
                this.raws = raws;
            }

            @Override
            public Selectable prepare(TableMetadata cfm)
            {
                return new WithSet(raws.stream().map(p -> p.prepare(cfm)).collect(Collectors.toList()));
            }
        }
    }

    /**
     * {@code Selectable} for literal Maps or UDTs.
     * <p>The parser cannot differentiate between a Map or a UDT in the selection cause because a
     * {@code ColumnIdentifier} is equivalent to a {@code FieldIdentifier} from a syntax point of view.
     * By consequence, we are forced to wait until the type is known to be able to differentiate them.</p>
     */
    public static class WithMapOrUdt implements Selectable
    {
        /**
         * The column family metadata. We need to store them to be able to build the proper data once the type has been
         * identified.
         */
        private final TableMetadata cfm;

        /**
         * The Map or UDT raw elements.
         */
        private final List<Pair<Selectable.Raw, Selectable.Raw>> raws;

        public WithMapOrUdt(TableMetadata cfm, List<Pair<Selectable.Raw, Selectable.Raw>> raws)
        {
            this.cfm = cfm;
            this.raws = raws;
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return receiver.type.isUDT() ? UserTypes.testUserTypeAssignment(receiver, getUdtFields((UserType) receiver.type))
                                         : Maps.testMapAssignment(receiver, getMapEntries(cfm));
        }

        @Override
        public Factory newSelectorFactory(TableMetadata cfm,
                                          AbstractType<?> expectedType,
                                          List<ColumnMetadata> defs,
                                          VariableSpecifications boundNames)
        {
            AbstractType<?> type = getExactTypeIfKnown(cfm.keyspace);
            if (type == null)
            {
                type = expectedType;
                if (type == null)
                    throw invalidRequest("Cannot infer type for term %s in selection clause (try using a cast to force a type)",
                                         this);
            }

            if (type.isUDT())
                return newUdtSelectorFactory(cfm, expectedType, defs, boundNames);

            return newMapSelectorFactory(cfm, defs, boundNames, type);
        }

        private Factory newMapSelectorFactory(TableMetadata cfm,
                                              List<ColumnMetadata> defs,
                                              VariableSpecifications boundNames,
                                              AbstractType<?> type)
        {
            MapType<?, ?> mapType = (MapType<?, ?>) type;

            if (mapType.getKeysType() == DurationType.instance)
                throw invalidRequest("Durations are not allowed as map keys: %s", mapType.asCQL3Type());

            return MapSelector.newFactory(type, getMapEntries(cfm).stream()
                                                                  .map(p -> Pair.create(p.left.newSelectorFactory(cfm, mapType.getKeysType(), defs, boundNames),
                                                                                        p.right.newSelectorFactory(cfm, mapType.getValuesType(), defs, boundNames)))
                                                                  .collect(Collectors.toList()));
        }

        private Factory newUdtSelectorFactory(TableMetadata cfm,
                                              AbstractType<?> expectedType,
                                              List<ColumnMetadata> defs,
                                              VariableSpecifications boundNames)
        {
            UserType ut = (UserType) expectedType;
            Map<FieldIdentifier, Factory> factories = new LinkedHashMap<>(ut.size());

            for (Pair<Selectable.Raw, Selectable.Raw> raw : raws)
            {
                if (!(raw.left instanceof RawIdentifier))
                    throw invalidRequest("%s is not a valid field identifier of type %s ",
                                         raw.left,
                                         ut.getNameAsString());

                FieldIdentifier fieldName = ((RawIdentifier) raw.left).toFieldIdentifier();
                int fieldPosition = ut.fieldPosition(fieldName);

                if (fieldPosition == -1)
                    throw invalidRequest("Unknown field '%s' in value of user defined type %s",
                                         fieldName,
                                         ut.getNameAsString());

                AbstractType<?> fieldType = ut.fieldType(fieldPosition);
                factories.put(fieldName,
                              raw.right.prepare(cfm).newSelectorFactory(cfm, fieldType, defs, boundNames));
            }

            return UserTypeSelector.newFactory(expectedType, factories);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            // Let's force the user to specify the type.
            return null;
        }

        @Override
        public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
        {
            // Let's force the user to specify the type.
            return null;
        }

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            for (Pair<Selectable.Raw, Selectable.Raw> raw : raws)
            {
                if (!(raw.left instanceof RawIdentifier) && raw.left.prepare(cfm).selectColumns(predicate))
                    return true;

                if (!raw.right.prepare(cfm).selectColumns(predicate))
                    return true;
            }
            return false;
        }

        @Override
        public String toString()
        {
            return raws.stream()
                       .map(p -> String.format("%s: %s",
                                               p.left instanceof RawIdentifier ? p.left : p.left.prepare(cfm),
                                               p.right.prepare(cfm)))
                       .collect(Collectors.joining(", ", "{", "}"));
        }

        private List<Pair<Selectable, Selectable>> getMapEntries(TableMetadata cfm)
        {
            return raws.stream()
                       .map(p -> Pair.create(p.left.prepare(cfm), p.right.prepare(cfm)))
                       .collect(Collectors.toList());
        }

        private Map<FieldIdentifier, Selectable> getUdtFields(UserType ut)
        {
            Map<FieldIdentifier, Selectable> fields = new LinkedHashMap<>(ut.size());

            for (Pair<Selectable.Raw, Selectable.Raw> raw : raws)
            {
                if (!(raw.left instanceof RawIdentifier))
                    throw invalidRequest("%s is not a valid field identifier of type %s ",
                                         raw.left,
                                         ut.getNameAsString());

                FieldIdentifier fieldName = ((RawIdentifier) raw.left).toFieldIdentifier();
                int fieldPosition = ut.fieldPosition(fieldName);

                if (fieldPosition == -1)
                    throw invalidRequest("Unknown field '%s' in value of user defined type %s",
                                         fieldName,
                                         ut.getNameAsString());

                fields.put(fieldName, raw.right.prepare(cfm));
            }

            return fields;
        }

        public static class Raw implements Selectable.Raw
        {
            private final List<Pair<Selectable.Raw, Selectable.Raw>> raws;

            public Raw(List<Pair<Selectable.Raw, Selectable.Raw>> raws)
            {
                this.raws = raws;
            }

            @Override
            public Selectable prepare(TableMetadata cfm)
            {
                return new WithMapOrUdt(cfm, raws);
            }
        }
    }

    /**
     * <code>Selectable</code> for type hints (e.g. (int) ?).
     */
    public static class WithTypeHint implements Selectable
    {

        /**
         * The name of the type as specified in the query.
         */
        private final String typeName;

        /**
         * The type specified by the hint.
         */
        private final AbstractType<?> type;

       /**
         * The selectable to which the hint should be applied.
         */
        private final Selectable selectable;

        public WithTypeHint(String typeName, AbstractType<?> type, Selectable selectable)
        {
            this.typeName = typeName;
            this.type = type;
            this.selectable = selectable;
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            if (receiver.type.equals(type))
                return AssignmentTestable.TestResult.EXACT_MATCH;
            else if (receiver.type.isValueCompatibleWith(type))
                return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
            else
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
        }

        @Override
        public Factory newSelectorFactory(TableMetadata cfm,
                                          AbstractType<?> expectedType,
                                          List<ColumnMetadata> defs,
                                          VariableSpecifications boundNames)
        {
            final ColumnSpecification receiver = new ColumnSpecification(cfm.keyspace, cfm.name, new ColumnIdentifier(toString(), true), type);

            if (!selectable.testAssignment(cfm.keyspace, receiver).isAssignable())
                throw new InvalidRequestException(String.format("Cannot assign value %s to %s of type %s", this, receiver.name, receiver.type.asCQL3Type()));

            final Factory factory = selectable.newSelectorFactory(cfm, type, defs, boundNames);

            return new ForwardingFactory()
            {
                protected Factory delegate()
                {
                    return factory;
                }

                protected AbstractType<?> getReturnType()
                {
                    return type;
                }

                protected String getColumnName()
                {
                    return String.format("(%s)%s", typeName, factory.getColumnName());
                }
            };
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return type;
        }

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            return selectable.selectColumns(predicate);
        }

        @Override
        public String toString()
        {
            return String.format("(%s)%s", typeName, selectable);
        }

        public static class Raw implements Selectable.Raw
        {
            private final CQL3Type.Raw typeRaw;

            private final Selectable.Raw raw;

            public Raw( CQL3Type.Raw typeRaw, Selectable.Raw raw)
            {
                this.typeRaw = typeRaw;
                this.raw = raw;
            }

            public Selectable prepare(TableMetadata cfm)
            {
                Selectable selectable = raw.prepare(cfm);
                AbstractType<?> type = this.typeRaw.prepare(cfm.keyspace).getType();
                if (type.isFreezable())
                    type = type.freeze();
                return new WithTypeHint(typeRaw.toString(), type, selectable);
            }
        }
    }

    /**
     * In the selection clause, the parser cannot differentiate between Maps and UDTs as a column identifier and field
     * identifier have the same syntax. By consequence, we need to wait until the type is known to create the proper
     * Object: {@code ColumnMetadata} or {@code FieldIdentifier}.
     */
    public static final class RawIdentifier implements Selectable.Raw
    {
        private final String text;

        private final boolean quoted;

        /**
         * Creates a {@code RawIdentifier} from an unquoted identifier string.
         */
        public static RawIdentifier forUnquoted(String text)
        {
            return new RawIdentifier(text, false);
        }

        /**
         * Creates a {@code RawIdentifier} from a quoted identifier string.
         */
        public static RawIdentifier forQuoted(String text)
        {
            return new RawIdentifier(text, true);
        }

        private RawIdentifier(String text, boolean quoted)
        {
            this.text = text;
            this.quoted = quoted;
        }

        public ColumnMetadata columnMetadata(TableMetadata cfm)
        {
            return cfm.getExistingColumn(ColumnIdentifier.getInterned(text, quoted));
        }

        @Override
        public Selectable prepare(TableMetadata cfm)
        {
            return columnMetadata(cfm);
        }

        public FieldIdentifier toFieldIdentifier()
        {
            return quoted ? FieldIdentifier.forQuoted(text)
                          : FieldIdentifier.forUnquoted(text);
        }

        @Override
        public String toString()
        {
            return text;
        }
    }

    /**
     * Represents the selection of an element of a collection (eg. c[x]).
     */
    public static class WithElementSelection implements Selectable
    {
        public final Selectable selected;
        // Note that we can't yet prepare the Term.Raw yet as we need the ColumnSpecificiation corresponding to Selectable, which
        // we'll only know in newSelectorFactory due to functions (which needs the defs passed to newSelectorFactory to resolve which
        // function is called). Note that this doesn't really matter performance wise since the factories are still created during
        // preparation of the corresponding SelectStatement.
        public final Term.Raw element;

        private WithElementSelection(Selectable selected, Term.Raw element)
        {
            assert element != null;
            this.selected = selected;
            this.element = element;
        }

        @Override
        public String toString()
        {
            return String.format("%s[%s]", selected, element);
        }

        public Selector.Factory newSelectorFactory(TableMetadata cfm, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames)
        {
            Selector.Factory factory = selected.newSelectorFactory(cfm, null, defs, boundNames);
            ColumnSpecification receiver = factory.getColumnSpecification(cfm);

            AbstractType<?> type = receiver.type;
            if (receiver.isReversedType())
            {
                type = ((ReversedType<?>) type).baseType;
            }
            if (!(type instanceof CollectionType))
                throw new InvalidRequestException(String.format("Invalid element selection: %s is of type %s is not a collection", selected, type.asCQL3Type()));

            ColumnSpecification boundSpec = specForElementOrSlice(selected, receiver, ((CollectionType) type).kind, "Element");

            Term elt = element.prepare(cfm.keyspace, boundSpec);
            elt.collectMarkerSpecification(boundNames);
            return ElementsSelector.newElementFactory(toString(), factory, (CollectionType)type, elt);
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            AbstractType<?> selectedType = selected.getExactTypeIfKnown(keyspace);
            if (selectedType == null || !(selectedType instanceof CollectionType))
                return null;

            return ElementsSelector.valueType((CollectionType) selectedType);
        }

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            return selected.selectColumns(predicate);
        }

        public static class Raw implements Selectable.Raw
        {
            private final Selectable.Raw selected;
            private final Term.Raw element;

            public Raw(Selectable.Raw selected, Term.Raw element)
            {
                this.selected = selected;
                this.element = element;
            }

            public WithElementSelection prepare(TableMetadata cfm)
            {
                return new WithElementSelection(selected.prepare(cfm), element);
            }

            @Override
            public String toString()
            {
                return String.format("%s[%s]", selected, element);
            }
        }
    }

    /**
     * Represents the selection of a slice of a collection (eg. c[x..y]).
     */
    public static class WithSliceSelection implements Selectable
    {
        public final Selectable selected;
        // Note that we can't yet prepare the Term.Raw yet as we need the ColumnSpecificiation corresponding to Selectable, which
        // we'll only know in newSelectorFactory due to functions (which needs the defs passed to newSelectorFactory to resolve which
        // function is called). Note that this doesn't really matter performance wise since the factories are still created during
        // preparation of the corresponding SelectStatement.
        // Both from and to can be null if they haven't been provided
        public final Term.Raw from;
        public final Term.Raw to;

        private WithSliceSelection(Selectable selected, Term.Raw from, Term.Raw to)
        {
            this.selected = selected;
            this.from = from;
            this.to = to;
        }

        @Override
        public String toString()
        {
            return String.format("%s[%s..%s]", selected, from == null ? "" : from, to == null ? "" : to);
        }

        public Selector.Factory newSelectorFactory(TableMetadata cfm, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames)
        {
            // Note that a slice gives you the same type as the collection you applied it to, so we can pass expectedType for selected directly
            Selector.Factory factory = selected.newSelectorFactory(cfm, expectedType, defs, boundNames);
            ColumnSpecification receiver = factory.getColumnSpecification(cfm);

            AbstractType<?> type = receiver.type;
            if (receiver.isReversedType())
            {
                type = ((ReversedType<?>) type).baseType;
            }
            if (!(type instanceof CollectionType))
                throw new InvalidRequestException(String.format("Invalid slice selection: %s of type %s is not a collection", selected, type.asCQL3Type()));

            ColumnSpecification boundSpec = specForElementOrSlice(selected, receiver, ((CollectionType) type).kind, "Slice");

            // If from or to are null, this means the user didn't provide on in the syntax (we had c[x..] or c[..x]).
            // The equivalent of doing this when preparing values would be to use UNSET.
            Term f = from == null ? Constants.UNSET_VALUE : from.prepare(cfm.keyspace, boundSpec);
            Term t = to == null ? Constants.UNSET_VALUE : to.prepare(cfm.keyspace, boundSpec);
            f.collectMarkerSpecification(boundNames);
            t.collectMarkerSpecification(boundNames);
            return ElementsSelector.newSliceFactory(toString(), factory, (CollectionType)type, f, t);
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            AbstractType<?> selectedType = selected.getExactTypeIfKnown(keyspace);
            if (selectedType == null || !(selectedType instanceof CollectionType))
                return null;

            return selectedType;
        }

        @Override
        public boolean selectColumns(Predicate<ColumnMetadata> predicate)
        {
            return selected.selectColumns(predicate);
        }

        public static class Raw implements Selectable.Raw
        {
            private final Selectable.Raw selected;
            // Both from and to can be null if they haven't been provided
            private final Term.Raw from;
            private final Term.Raw to;

            public Raw(Selectable.Raw selected, Term.Raw from, Term.Raw to)
            {
                this.selected = selected;
                this.from = from;
                this.to = to;
            }

            public WithSliceSelection prepare(TableMetadata cfm)
            {
                return new WithSliceSelection(selected.prepare(cfm), from, to);
            }

            @Override
            public String toString()
            {
                return String.format("%s[%s..%s]", selected, from == null ? "" : from, to == null ? "" : to);
            }
        }
    }
}
