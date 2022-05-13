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

package org.apache.cassandra.cql3.transactions;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.types.utils.Bytes;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.service.accord.txn.TxnDataName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.accord.txn.TxnReference;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

public class RowDataReference extends Term.NonTerminal
{
    public static final String CANNOT_FIND_TUPLE_MESSAGE = "Cannot resolve reference to tuple '%s'.";
    public static final String COLUMN_NOT_IN_TUPLE_MESSAGE = "Column '%s' does not exist in tuple '%s'.";

    private final TxnDataName selectName;
    private final ColumnMetadata column;
    private final Term elementPath;
    private final CellPath fieldPath;
    
    public RowDataReference(TxnDataName selectName, ColumnMetadata column, Term elementPath, CellPath fieldPath)
    {
        Preconditions.checkArgument(elementPath == null || fieldPath == null, "Cannot specify both element and field paths");
        
        this.selectName = selectName;
        this.column = column;
        this.elementPath = elementPath;
        this.fieldPath = fieldPath;
    }

    @Override
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (elementPath != null)
            elementPath.collectMarkerSpecification(boundNames);
    }

    @Override
    public Terminal bind(QueryOptions options) throws InvalidRequestException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsBindMarker()
    {
        return elementPath != null && elementPath.containsBindMarker();
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        throw new UnsupportedOperationException("Functions are not currently supported w/ reference terms.");
    }

    public ColumnMetadata toResultMetadata()
    {
        ColumnIdentifier fullName = getFullyQualifiedName();
        ColumnMetadata forMetadata = column.withNewName(fullName);

        if (isElementSelection())
        {
            if (forMetadata.type instanceof ListType)
                forMetadata = forMetadata.withNewType(((ListType<?>) forMetadata.type).valueComparator());
            else if (forMetadata.type instanceof SetType)
                forMetadata = forMetadata.withNewType(((SetType<?>) forMetadata.type).nameComparator());
            else if (forMetadata.type instanceof MapType)
                forMetadata = forMetadata.withNewType(((MapType<?, ?>) forMetadata.type).valueComparator());
        }
        else if (isFieldSelection())
        {
            forMetadata = forMetadata.withNewType(getFieldSelectionType());
        }
        return forMetadata;
    }

    public ColumnSpecification getValueReceiver()
    {
        if (isElementSelection())
        {
            CollectionType.Kind collectionKind = ((CollectionType<?>) column.type).kind;
            switch (collectionKind)
            {
                case LIST:
                    return Lists.valueSpecOf(column);
                case MAP:
                    return Maps.valueSpecOf(column);
                default:
                    throw new InvalidRequestException(String.format("Element selection not supported for column %s of type %s" ,
                                                                    column.name, collectionKind));
            }
        }
        else if (isFieldSelection())
        {
            return getFieldSelectionSpec();
        }

        return column;
    }

    public boolean isElementSelection()
    {
        return elementPath != null && column.type.isCollection();
    }

    public boolean isFieldSelection()
    {
        return fieldPath != null && column.type.isUDT();
    }

    private AbstractType<?> getFieldSelectionType()
    {
        assert isFieldSelection() : "No field selection type exists";
        return getFieldSelectionType(column, fieldPath);
    }

    private static AbstractType<?> getFieldSelectionType(ColumnMetadata column, CellPath fieldPath)
    {
        return ((UserType) column.type).fieldType(fieldPath);
    }

    public ColumnSpecification getFieldSelectionSpec()
    {
        assert isFieldSelection() : "No field selection type exists";
        int field = ByteBufferUtil.getUnsignedShort(fieldPath.get(0), 0);
        return UserTypes.fieldSpecOf(column, field);
    }

    private CellPath bindCellPath(QueryOptions options)
    {
        if (fieldPath != null)
            return fieldPath;

        return elementPath != null ? CellPath.create(elementPath.bindAndGet(options)) : null;
    }

    public TxnReference toTxnReference(QueryOptions options)
    {
        Preconditions.checkState(elementPath == null || column.isComplex() || column.type.isFrozenCollection());
        Preconditions.checkState(fieldPath == null || column.isComplex() || column.type.isUDT());
        return new TxnReference(selectName, column, bindCellPath(options));
    }

    public ColumnIdentifier getFullyQualifiedName()
    {
        // TODO: Make this more user-friendly...
        String path = fieldPath != null ? '.' + Bytes.toHexString(fieldPath.get(0)) : (elementPath == null ? "" : "[0x" + elementPath + ']');
        String fullName = selectName.name() + '.' + column.name.toString() + path;
        return new ColumnIdentifier(fullName, true);
    }

    public ColumnMetadata column()
    {
        return column;
    }

    public static class Raw extends Term.Raw
    {
        private final Selectable.RawIdentifier tuple;
        private final Selectable.RawIdentifier selected;
        private final Object fieldOrElement;
        
        private boolean isResolved = false;

        private TxnDataName tupleName;
        private ColumnMetadata column;
        private Term elementPath = null;
        private CellPath fieldPath = null;

        public Raw(Selectable.RawIdentifier tuple, Selectable.Raw selected, Object fieldOrElement)
        {
            Preconditions.checkArgument(tuple != null, "tuple is null");
            Preconditions.checkArgument(selected == null || selected instanceof Selectable.RawIdentifier, "selected is not a Selectable.RawIdentifier: " + selected);
            this.tuple = tuple;
            this.selected = (Selectable.RawIdentifier) selected;
            this.fieldOrElement = fieldOrElement;
        }

        public static Raw fromSelectable(Selectable.RawIdentifier tuple, Selectable.Raw selectable)
        {
            if (selectable == null)
                return new RowDataReference.Raw(tuple, null, null);

            // TODO: Ideally it would be nice not to have to make items in the Selectables public
            if (selectable instanceof Selectable.WithFieldSelection.Raw)
            {
                Selectable.WithFieldSelection.Raw selection = (Selectable.WithFieldSelection.Raw) selectable;
                return new RowDataReference.Raw(tuple, selection.selected, selection.field);
            }
            else if (selectable instanceof Selectable.WithElementSelection.Raw)
            {
                Selectable.WithElementSelection.Raw elementSelection = (Selectable.WithElementSelection.Raw) selectable;
                return new RowDataReference.Raw(tuple, elementSelection.selected, elementSelection.element);
            }
            else if (selectable instanceof Selectable.RawIdentifier)
            {
                Selectable.RawIdentifier selection = (Selectable.RawIdentifier) selectable;
                return new RowDataReference.Raw(tuple, selection, null);
            }

            throw new UnsupportedOperationException("Cannot create column reference from selectable: " + selectable);
        }

        private void resolveFinished()
        {
            isResolved = true;
        }

        public void resolveReference(Map<TxnDataName, ReferenceSource> sources)
        {
            if (isResolved)
                return;

            // root level name
            tupleName = TxnDataName.user(tuple.toString());
            ReferenceSource source = sources.get(tupleName);
            checkNotNull(source, CANNOT_FIND_TUPLE_MESSAGE, tupleName.name());
            
            if (selected == null)
            {
                resolveFinished();
                return;
            }

            column = source.getColumn(selected.toString());
            checkNotNull(column, COLUMN_NOT_IN_TUPLE_MESSAGE, selected.toString(), tupleName.name());

            // TODO: confirm update partition key terms don't contain column references. This can't be done in prepare
            //   because there can be intermediate functions (ie: pk=row.v+1 or pk=_add(row.v, 5)). Need a recursive Term visitor

            if (fieldOrElement == null)
            {
                resolveFinished();
                return;
            }

            if (column.type.isCollection())
            {
                Term.Raw element = (Term.Raw) fieldOrElement;
                elementPath = element.prepare(column.ksName, specForElementOrSlice(column));
            }
            else if (column.type.isUDT())
            {
                FieldIdentifier field = (FieldIdentifier) fieldOrElement;
                UserType userType = (UserType) column.type;
                fieldPath = userType.cellPathForField(field);
            }

            resolveFinished();
        }

        private ColumnSpecification specForElementOrSlice(ColumnSpecification receiver)
        {
            switch (((CollectionType<?>) receiver.type).kind)
            {
                case LIST: return Lists.indexSpecOf(receiver);
                case SET: return Sets.valueSpecOf(receiver);
                case MAP: return Maps.keySpecOf(receiver);
                default: throw new AssertionError("Unknown collection type: " + receiver.type);
            }
        }

        public void checkResolved()
        {
            if (!isResolved)
                throw new IllegalStateException();
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            checkResolved();
            
            AbstractType<?> type = column.type;
            
            if (elementPath != null)
            {
                CollectionType<?> collectionType = (CollectionType<?>) type;
                type = collectionType.kind == CollectionType.Kind.SET ? collectionType.nameComparator() : collectionType.valueComparator();
            }
            else if (fieldPath != null)
            {
                type = RowDataReference.getFieldSelectionType(column, fieldPath);
            }
            
            return type.testAssignment(receiver.type);
        }

        @Override
        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            return prepare(keyspace, receiver, tupleName, column, elementPath, fieldPath);
        }

        public RowDataReference prepareAsReceiver()
        {
            checkResolved();
            return new RowDataReference(tupleName, column, elementPath, fieldPath);
        }

        private RowDataReference prepare(String keyspace,
                                         ColumnSpecification receiver,
                                         TxnDataName selectName,
                                         ColumnMetadata column,
                                         Term elementPath,
                                         CellPath fieldPath)
        {
            if (!testAssignment(keyspace, receiver).isAssignable())
                throw new InvalidRequestException(String.format("Invalid reference type %s (%s) for \"%s\" of type %s",
                                                                column.type, column.name, receiver.name, receiver.type.asCQL3Type()));

            return new RowDataReference(selectName, column, elementPath, fieldPath);
        }

        @Override
        public String getText()
        {
            StringBuilder text = new StringBuilder(tuple.toString());

            if (selected != null)
                text.append('.').append(selected);

            if (fieldOrElement != null)
            {
                if (fieldOrElement instanceof Term.Raw)
                {
                    Term.Raw element = (Term.Raw) fieldOrElement;
                    text.append('.').append(element.getText());
                }
                else if (fieldOrElement instanceof FieldIdentifier)
                {
                    FieldIdentifier field = (FieldIdentifier) fieldOrElement;
                    text.append('.').append(field);
                }
                else
                {
                    throw new IllegalStateException("Field or element is neither a raw term nor a field identifier");
                }
            }

            return text.toString();
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            checkResolved();
            return column.type;
        }

        public ColumnMetadata column()
        {
            return column;
        }
    }

    public interface ReferenceSource
    {
        ColumnMetadata getColumn(String name);
    }
}
