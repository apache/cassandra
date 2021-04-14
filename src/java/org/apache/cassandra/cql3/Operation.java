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
package org.apache.cassandra.cql3;

import java.util.List;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * An UPDATE or DELETE operation.
 *
 * For UPDATE this includes:
 *   - setting a constant
 *   - counter operations
 *   - collections operations
 * and for DELETE:
 *   - deleting a column
 *   - deleting an element of collection column
 *
 * Fine grained operation are obtained from their raw counterpart (Operation.Raw, which
 * correspond to a parsed, non-checked operation) by provided the receiver for the operation.
 */
public abstract class Operation
{
    // the column the operation applies to
    public final ColumnMetadata column;

    // Term involved in the operation. In theory this should not be here since some operation
    // may require none of more than one term, but most need 1 so it simplify things a bit.
    protected final Term t;

    protected Operation(ColumnMetadata column, Term t)
    {
        assert column != null;
        this.column = column;
        this.t = t;
    }

    public void addFunctionsTo(List<Function> functions)
    {
        if (t != null)
            t.addFunctionsTo(functions);
    }

    /**
     * @return whether the operation requires a read of the previous value to be executed
     * (only lists setterByIdx, discard and discardByIdx requires that).
     */
    public boolean requiresRead()
    {
        return false;
    }

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (t != null)
            t.collectMarkerSpecification(boundNames);
    }

    /**
     * Execute the operation.
     *
     * @param partitionKey partition key for the update.
     * @param params parameters of the update.
     */
    public abstract void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException;

    /**
     * A parsed raw UPDATE operation.
     *
     * This can be one of:
     *   - Setting a value: c = v
     *   - Setting an element of a collection: c[x] = v
     *   - An addition/subtraction to a variable: c = c +/- v (where v can be a collection literal, scalar, or string)
     *   - An prepend operation: c = v + c
     */
    public interface RawUpdate
    {
        /**
         * This method validates the operation (i.e. validate it is well typed)
         * based on the specification of the receiver of the operation.
         *
         * It returns an Operation which can be though as post-preparation well-typed
         * Operation.
         *
         *
         * @param metadata
         * @param receiver the column this operation applies to.
         * @param canReadExistingState whether the update depends on existing state
         *                 
         * @return the prepared update operation.
         */
        public Operation prepare(TableMetadata metadata, ColumnMetadata receiver, boolean canReadExistingState) throws InvalidRequestException;

        /**
         * @return whether this operation can be applied alongside the {@code
         * other} update (in the same UPDATE statement for the same column).
         */
        public boolean isCompatibleWith(RawUpdate other);
    }

    /**
     * A parsed raw DELETE operation.
     *
     * This can be one of:
     *   - Deleting a column
     *   - Deleting an element of a collection
     */
    public interface RawDeletion
    {
        /**
         * The name of the column affected by this delete operation.
         */
        public ColumnIdentifier affectedColumn();

        /**
         * This method validates the operation (i.e. validate it is well typed)
         * based on the specification of the column affected by the operation (i.e the
         * one returned by affectedColumn()).
         *
         * It returns an Operation which can be though as post-preparation well-typed
         * Operation.
         *
         * @param receiver the "column" this operation applies to.
         * @param metadata
         * @return the prepared delete operation.
         */
        public Operation prepare(String keyspace, ColumnMetadata receiver, TableMetadata metadata) throws InvalidRequestException;
    }

    public static class SetValue implements RawUpdate
    {
        private final Term.Raw value;

        public SetValue(Term.Raw value)
        {
            this.value = value;
        }

        public Operation prepare(TableMetadata metadata, ColumnMetadata receiver, boolean canReadExistingState) throws InvalidRequestException
        {
            Term v = value.prepare(metadata.keyspace, receiver);

            if (receiver.type instanceof CounterColumnType)
                throw new InvalidRequestException(String.format("Cannot set the value of counter column %s (counters can only be incremented/decremented, not set)", receiver.name));

            if (receiver.type.isCollection())
            {
                switch (((CollectionType) receiver.type).kind)
                {
                    case LIST:
                        return new Lists.Setter(receiver, v);
                    case SET:
                        return new Sets.Setter(receiver, v);
                    case MAP:
                        return new Maps.Setter(receiver, v);
                    default:
                        throw new AssertionError();
                }
            }

            if (receiver.type.isUDT())
                return new UserTypes.Setter(receiver, v);

            return new Constants.Setter(receiver, v);
        }

        protected String toString(ColumnSpecification column)
        {
            return String.format("%s = %s", column, value);
        }

        public boolean isCompatibleWith(RawUpdate other)
        {
            // We don't allow setting multiple time the same column, because 1)
            // it's stupid and 2) the result would seem random to the user.
            return false;
        }
    }

    public static class SetElement implements RawUpdate
    {
        private final Term.Raw selector;
        private final Term.Raw value;

        public SetElement(Term.Raw selector, Term.Raw value)
        {
            this.selector = selector;
            this.value = value;
        }

        public Operation prepare(TableMetadata metadata, ColumnMetadata receiver, boolean canReadExistingState) throws InvalidRequestException
        {
            if (!(receiver.type instanceof CollectionType))
                throw new InvalidRequestException(String.format("Invalid operation (%s) for non collection column %s", toString(receiver), receiver.name));
            else if (!(receiver.type.isMultiCell()))
                throw new InvalidRequestException(String.format("Invalid operation (%s) for frozen collection column %s", toString(receiver), receiver.name));

            switch (((CollectionType)receiver.type).kind)
            {
                case LIST:
                    Term idx = selector.prepare(metadata.keyspace, Lists.indexSpecOf(receiver));
                    Term lval = value.prepare(metadata.keyspace, Lists.valueSpecOf(receiver));
                    return new Lists.SetterByIndex(receiver, idx, lval);
                case SET:
                    throw new InvalidRequestException(String.format("Invalid operation (%s) for set column %s", toString(receiver), receiver.name));
                case MAP:
                    Term key = selector.prepare(metadata.keyspace, Maps.keySpecOf(receiver));
                    Term mval = value.prepare(metadata.keyspace, Maps.valueSpecOf(receiver));
                    return new Maps.SetterByKey(receiver, key, mval);
            }
            throw new AssertionError();
        }

        protected String toString(ColumnSpecification column)
        {
            return String.format("%s[%s] = %s", column.name, selector, value);
        }

        public boolean isCompatibleWith(RawUpdate other)
        {
            // TODO: we could check that the other operation is not setting the same element
            // too (but since the index/key set may be a bind variables we can't always do it at this point)
            return !(other instanceof SetValue);
        }
    }

    public static class SetField implements RawUpdate
    {
        private final FieldIdentifier field;
        private final Term.Raw value;

        public SetField(FieldIdentifier field, Term.Raw value)
        {
            this.field = field;
            this.value = value;
        }

        public Operation prepare(TableMetadata metadata, ColumnMetadata receiver, boolean canReadExistingState) throws InvalidRequestException
        {
            if (!receiver.type.isUDT())
                throw new InvalidRequestException(String.format("Invalid operation (%s) for non-UDT column %s", toString(receiver), receiver.name));
            else if (!receiver.type.isMultiCell())
                throw new InvalidRequestException(String.format("Invalid operation (%s) for frozen UDT column %s", toString(receiver), receiver.name));

            int fieldPosition = ((UserType) receiver.type).fieldPosition(field);
            if (fieldPosition == -1)
                throw new InvalidRequestException(String.format("UDT column %s does not have a field named %s", receiver.name, field));

            Term val = value.prepare(metadata.keyspace, UserTypes.fieldSpecOf(receiver, fieldPosition));
            return new UserTypes.SetterByField(receiver, field, val);
        }

        protected String toString(ColumnSpecification column)
        {
            return String.format("%s.%s = %s", column.name, field, value);
        }

        public boolean isCompatibleWith(RawUpdate other)
        {
            if (other instanceof SetField)
                return !((SetField) other).field.equals(field);
            else
                return !(other instanceof SetValue);
        }
    }

    public static class Addition implements RawUpdate
    {
        private final Term.Raw value;

        public Addition(Term.Raw value)
        {
            this.value = value;
        }

        public Operation prepare(TableMetadata metadata, ColumnMetadata receiver, boolean canReadExistingState) throws InvalidRequestException
        {
            if (!(receiver.type instanceof CollectionType))
            {
                if (receiver.type instanceof TupleType)
                    throw new InvalidRequestException(String.format("Invalid operation (%s) for tuple column %s", toString(receiver), receiver.name));

                if (canReadExistingState)
                {
                    if (!(receiver.type instanceof NumberType<?>) && !(receiver.type instanceof StringType))
                        throw new InvalidRequestException(String.format("Invalid operation (%s) for non-numeric and non-text type %s", toString(receiver), receiver.name));
                }
                else
                {
                    if (!(receiver.type instanceof CounterColumnType))
                        throw new InvalidRequestException(String.format("Invalid operation (%s) for non counter column %s", toString(receiver), receiver.name));
                }
                return new Constants.Adder(receiver, value.prepare(metadata.keyspace, receiver));
            }
            else if (!(receiver.type.isMultiCell()))
                throw new InvalidRequestException(String.format("Invalid operation (%s) for frozen collection column %s", toString(receiver), receiver.name));

            switch (((CollectionType)receiver.type).kind)
            {
                case LIST:
                    return new Lists.Appender(receiver, value.prepare(metadata.keyspace, receiver));
                case SET:
                    return new Sets.Adder(receiver, value.prepare(metadata.keyspace, receiver));
                case MAP:
                    Term term;
                    try
                    {
                        term = value.prepare(metadata.keyspace, receiver);
                    }
                    catch (InvalidRequestException e)
                    {
                        throw new InvalidRequestException(String.format("Value for a map addition has to be a map, but was: '%s'", value));
                    }

                    return new Maps.Putter(receiver, term);
            }
            throw new AssertionError();
        }

        protected String toString(ColumnSpecification column)
        {
            return String.format("%s = %s + %s", column.name, column.name, value);
        }

        public boolean isCompatibleWith(RawUpdate other)
        {
            return !(other instanceof SetValue);
        }
    }

    public static class Substraction implements RawUpdate
    {
        private final Term.Raw value;

        public Substraction(Term.Raw value)
        {
            this.value = value;
        }

        public Operation prepare(TableMetadata metadata, ColumnMetadata receiver, boolean canReadExistingState) throws InvalidRequestException
        {
            if (!(receiver.type instanceof CollectionType))
            {
                if (!(receiver.type instanceof CounterColumnType))
                    throw new InvalidRequestException(String.format("Invalid operation (%s) for non counter column %s", toString(receiver), receiver.name));
                return new Constants.Substracter(receiver, value.prepare(metadata.keyspace, receiver));
            }
            else if (!(receiver.type.isMultiCell()))
                throw new InvalidRequestException(String.format("Invalid operation (%s) for frozen collection column %s", toString(receiver), receiver.name));

            switch (((CollectionType)receiver.type).kind)
            {
                case LIST:
                    return new Lists.Discarder(receiver, value.prepare(metadata.keyspace, receiver));
                case SET:
                    return new Sets.Discarder(receiver, value.prepare(metadata.keyspace, receiver));
                case MAP:
                    // The value for a map subtraction is actually a set
                    ColumnSpecification vr = new ColumnSpecification(receiver.ksName,
                                                                     receiver.cfName,
                                                                     receiver.name,
                                                                     SetType.getInstance(((MapType)receiver.type).getKeysType(), false));
                    Term term;
                    try
                    {
                        term = value.prepare(metadata.keyspace, vr);
                    }
                    catch (InvalidRequestException e)
                    {
                        throw new InvalidRequestException(String.format("Value for a map substraction has to be a set, but was: '%s'", value));
                    }
                    return new Sets.Discarder(receiver, term);
            }
            throw new AssertionError();
        }

        protected String toString(ColumnSpecification column)
        {
            return String.format("%s = %s - %s", column.name, column.name, value);
        }

        public boolean isCompatibleWith(RawUpdate other)
        {
            return !(other instanceof SetValue);
        }
    }

    public static class Prepend implements RawUpdate
    {
        private final Term.Raw value;

        public Prepend(Term.Raw value)
        {
            this.value = value;
        }

        public Operation prepare(TableMetadata metadata, ColumnMetadata receiver, boolean canReadExistingState) throws InvalidRequestException
        {
            Term v = value.prepare(metadata.keyspace, receiver);

            if (!(receiver.type instanceof ListType))
                throw new InvalidRequestException(String.format("Invalid operation (%s) for non list column %s", toString(receiver), receiver.name));
            else if (!(receiver.type.isMultiCell()))
                throw new InvalidRequestException(String.format("Invalid operation (%s) for frozen list column %s", toString(receiver), receiver.name));

            return new Lists.Prepender(receiver, v);
        }

        protected String toString(ColumnSpecification column)
        {
            return String.format("%s = %s - %s", column.name, value, column.name);
        }

        public boolean isCompatibleWith(RawUpdate other)
        {
            return !(other instanceof SetValue);
        }
    }

    public static class ColumnDeletion implements RawDeletion
    {
        private final ColumnIdentifier id;

        public ColumnDeletion(ColumnIdentifier id)
        {
            this.id = id;
        }

        public ColumnIdentifier affectedColumn()
        {
            return id;
        }

        public Operation prepare(String keyspace, ColumnMetadata receiver, TableMetadata metadata) throws InvalidRequestException
        {
            // No validation, deleting a column is always "well typed"
            return new Constants.Deleter(receiver);
        }
    }

    public static class ElementDeletion implements RawDeletion
    {
        private final ColumnIdentifier id;
        private final Term.Raw element;

        public ElementDeletion(ColumnIdentifier id, Term.Raw element)
        {
            this.id = id;
            this.element = element;
        }

        public ColumnIdentifier affectedColumn()
        {
            return id;
        }

        public Operation prepare(String keyspace, ColumnMetadata receiver, TableMetadata metadata) throws InvalidRequestException
        {
            if (!(receiver.type.isCollection()))
                throw new InvalidRequestException(String.format("Invalid deletion operation for non collection column %s", receiver.name));
            else if (!(receiver.type.isMultiCell()))
                throw new InvalidRequestException(String.format("Invalid deletion operation for frozen collection column %s", receiver.name));

            switch (((CollectionType)receiver.type).kind)
            {
                case LIST:
                    Term idx = element.prepare(keyspace, Lists.indexSpecOf(receiver));
                    return new Lists.DiscarderByIndex(receiver, idx);
                case SET:
                    Term elt = element.prepare(keyspace, Sets.valueSpecOf(receiver));
                    return new Sets.ElementDiscarder(receiver, elt);
                case MAP:
                    Term key = element.prepare(keyspace, Maps.keySpecOf(receiver));
                    return new Maps.DiscarderByKey(receiver, key);
            }
            throw new AssertionError();
        }
    }

    public static class FieldDeletion implements RawDeletion
    {
        private final ColumnIdentifier id;
        private final FieldIdentifier field;

        public FieldDeletion(ColumnIdentifier id, FieldIdentifier field)
        {
            this.id = id;
            this.field = field;
        }

        public ColumnIdentifier affectedColumn()
        {
            return id;
        }

        public Operation prepare(String keyspace, ColumnMetadata receiver, TableMetadata metadata) throws InvalidRequestException
        {
            if (!receiver.type.isUDT())
                throw new InvalidRequestException(String.format("Invalid field deletion operation for non-UDT column %s", receiver.name));
            else if (!receiver.type.isMultiCell())
                throw new InvalidRequestException(String.format("Frozen UDT column %s does not support field deletions", receiver.name));

            if (((UserType) receiver.type).fieldPosition(field) == -1)
                throw new InvalidRequestException(String.format("UDT column %s does not have a field named %s", receiver.name, field));

            return new UserTypes.DeleterByField(receiver, field);
        }
    }
}
