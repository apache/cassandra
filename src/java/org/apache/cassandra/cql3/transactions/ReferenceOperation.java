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

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.txn.TxnReferenceOperation;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.db.marshal.CollectionType.Kind.MAP;
import static org.apache.cassandra.schema.TableMetadata.UNDEFINED_COLUMN_NAME_MESSAGE;

public class ReferenceOperation
{
    private final ColumnMetadata receiver;
    private final TxnReferenceOperation.Kind kind;
    private final FieldIdentifier field;
    private final Term key;
    private final ReferenceValue value;

    public ReferenceOperation(ColumnMetadata receiver, TxnReferenceOperation.Kind kind, Term key, FieldIdentifier field, ReferenceValue value)
    {
        this.receiver = receiver;
        this.kind = kind;
        this.key = key;
        this.field = field;
        this.value = value;
    }

    /**
     * Creates a {@link ReferenceOperation} from the given {@link  Operation} for the purpose of defering execution
     * within a transaction. When the language sees an Operation using a reference one is created already, but for cases
     * that needs to defer execution (such as when {@link Operation#requiresRead()} is true), this method can be used.
     */
    public static ReferenceOperation create(Operation operation)
    {
        TxnReferenceOperation.Kind kind = TxnReferenceOperation.Kind.from(operation);
        ColumnMetadata receiver = operation.column;
        
        // We already have a prepared reference value, so there is no need to inspect the value type.
        ReferenceValue value = new ReferenceValue.Constant(operation.term());
        Term key = extractKeyOrIndex(operation);
        FieldIdentifier field = extractField(operation);
        return new ReferenceOperation(receiver, kind, key, field, value);
    }

    public ColumnMetadata getReceiver()
    {
        return receiver;
    }

    public boolean requiresRead()
    {
        // TODO: Find a better way than delegating to the operation?
        return kind.toOperation(receiver, null, null, null).requiresRead();
    }

    public TxnReferenceOperation bindAndGet(QueryOptions options)
    {
        return new TxnReferenceOperation(kind,
                                         receiver,
                                         key != null ? key.bindAndGet(options) : null,
                                         field != null ? field.bytes : null,
                                         value.bindAndGet(options));
    }

    public static class Raw
    {
        private final Operation.RawUpdate rawUpdate;
        public final ColumnIdentifier column;
        private final ReferenceValue.Raw value;

        public Raw(Operation.RawUpdate rawUpdate, ColumnIdentifier column, ReferenceValue.Raw value)
        {
            this.rawUpdate = rawUpdate;
            this.column = column;
            this.value = value;
        }

        public ReferenceOperation prepare(TableMetadata metadata, VariableSpecifications bindVariables)
        {
            ColumnMetadata receiver = metadata.getColumn(column);
            Operation operation = rawUpdate.prepare(metadata, receiver, true);
            TxnReferenceOperation.Kind kind = TxnReferenceOperation.Kind.from(operation);
            Term key = extractKeyOrIndex(operation);
            
            checkTrue(receiver != null, UNDEFINED_COLUMN_NAME_MESSAGE, column.toCQLString(), metadata);
            AbstractType<?> type = receiver.type;
            ColumnMetadata valueReceiver = receiver;

            if (type.isCollection())
            {
                CollectionType<?> collectionType = (CollectionType<?>) type;

                // The value for a map subtraction is actually a set (see Operation.Substraction)
                if (kind == TxnReferenceOperation.Kind.SetDiscarder && collectionType.kind == MAP)
                    valueReceiver = valueReceiver.withNewType(SetType.getInstance(((MapType<?, ?>) type).getKeysType(), true));

                if (kind == TxnReferenceOperation.Kind.ListSetterByIndex || kind == TxnReferenceOperation.Kind.MapSetterByKey)
                    valueReceiver = valueReceiver.withNewType(collectionType.valueComparator());
            }

            FieldIdentifier field = extractField(operation);

            if (type.isUDT())
            {
                if (kind == TxnReferenceOperation.Kind.UserTypeSetterByField)
                {
                    @SuppressWarnings("ConstantConditions") UserType userType = (UserType) type;
                    CellPath fieldPath = userType.cellPathForField(field);
                    int i = ByteBufferUtil.getUnsignedShort(fieldPath.get(0), 0);
                    valueReceiver = valueReceiver.withNewType(userType.fieldType(i));
                }
            }

            return new ReferenceOperation(receiver, kind, key, field, value.prepare(valueReceiver, bindVariables));
        }
    }

    private static FieldIdentifier extractField(Operation operation)
    {
        if (operation instanceof UserTypes.SetterByField)
            return ((UserTypes.SetterByField) operation).field;
        return null;
    }

    private static Term extractKeyOrIndex(Operation operation)
    {
        // TODO: Is there a way to do this without exposing k and idx?
        if (operation instanceof Maps.SetterByKey)
            return ((Maps.SetterByKey) operation).k;
        else if (operation instanceof Lists.SetterByIndex)
            return ((Lists.SetterByIndex) operation).idx;
        return null;
    }
}
