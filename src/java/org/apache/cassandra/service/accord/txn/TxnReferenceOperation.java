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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.accord.AccordSerializers;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.marshal.CollectionType.Kind.MAP;
import static org.apache.cassandra.service.accord.AccordSerializers.columnMetadataSerializer;

public class TxnReferenceOperation
{
    private static final Map<Class<? extends Operation>, Kind> operationKindMap = initOperationKindMap();
    
    private static Map<Class<? extends Operation>, Kind> initOperationKindMap()
    {
        Map<Class<? extends Operation>, Kind> temp = new HashMap<>();
        temp.put(Sets.Adder.class, Kind.SetAdder);
        temp.put(Constants.Adder.class, Kind.ConstantAdder);
        temp.put(Lists.Appender.class, Kind.ListAppender);
        temp.put(Sets.Discarder.class, Kind.SetDiscarder);
        temp.put(Lists.Discarder.class, Kind.ListDiscarder);
        temp.put(Lists.Prepender.class, Kind.ListPrepender);
        temp.put(Maps.Putter.class, Kind.MapPutter);
        temp.put(Lists.Setter.class, Kind.ListSetter);
        temp.put(Sets.Setter.class, Kind.SetSetter);
        temp.put(Maps.Setter.class, Kind.MapSetter);
        temp.put(UserTypes.Setter.class, Kind.UserTypeSetter);
        temp.put(Constants.Setter.class, Kind.ConstantSetter);
        temp.put(Constants.Substracter.class, Kind.ConstantSubtracter);
        temp.put(Maps.SetterByKey.class, Kind.MapSetterByKey);
        temp.put(Lists.SetterByIndex.class, Kind.ListSetterByIndex);
        temp.put(UserTypes.SetterByField.class, Kind.UserTypeSetterByField);
        return temp;
    }

    private interface ToOperation
    {
        Operation apply(ColumnMetadata column, Term keyOrIndex, FieldIdentifier field, Term value);
    }

    public enum Kind
    {
        SetAdder((byte) 1, (column, keyOrIndex, field, value) -> new Sets.Adder(column, value)),
        ConstantAdder((byte) 2, (column, keyOrIndex, field, value) -> new Constants.Adder(column, value)),
        ListAppender((byte) 3, (column, keyOrIndex, field, value) -> new Lists.Appender(column, value)),
        SetDiscarder((byte) 4, (column, keyOrIndex, field, value) -> new Sets.Discarder(column, value)),
        ListDiscarder((byte) 5, (column, keyOrIndex, field, value) -> new Lists.Discarder(column, value)),
        ListPrepender((byte) 6, (column, keyOrIndex, field, value) -> new Lists.Prepender(column, value)),
        MapPutter((byte) 7, (column, keyOrIndex, field, value) -> new Maps.Putter(column, value)),
        ListSetter((byte) 8, (column, keyOrIndex, field, value) -> new Lists.Setter(column, value)),
        SetSetter((byte) 9, (column, keyOrIndex, field, value) -> new Sets.Setter(column, value)),
        MapSetter((byte) 10, (column, keyOrIndex, field, value) -> new Maps.Setter(column, value)),
        UserTypeSetter((byte) 11, (column, keyOrIndex, field, value) -> new UserTypes.Setter(column, value)),
        ConstantSetter((byte) 12, (column, keyOrIndex, field, value) -> new Constants.Setter(column, value)),
        ConstantSubtracter((byte) 13, (column, keyOrIndex, field, value) -> new Constants.Substracter(column, value)),
        MapSetterByKey((byte) 14, (column, keyOrIndex, field, value) -> new Maps.SetterByKey(column, keyOrIndex, value)),
        ListSetterByIndex((byte) 15, (column, keyOrIndex, field, value) -> new Lists.SetterByIndex(column, keyOrIndex, value)),
        UserTypeSetterByField((byte) 16, (column, keyOrIndex, field, value) -> new UserTypes.SetterByField(column, field, value));

        private final byte id;
        private final ToOperation toOperation;

        Kind(byte id, ToOperation toOperation)
        {
            this.id = id;
            this.toOperation = toOperation;
        }

        public static Kind from(byte b)
        {
            for (Kind k : values())
                if (k.id == b)
                    return k;

            throw new IllegalArgumentException("There is no kind with id: " + b);
        }

        public static Kind from(Operation operation)
        {
            Class<? extends Operation> clazz = operation.getClass();
            Kind kind = operationKindMap.get(clazz);
            if (kind == null)
                throw new IllegalArgumentException("There is no Kind associated with operation: " + clazz);
            return kind;
        }
        
        public static Kind setterFor(ColumnMetadata column)
        {
            if (column.type instanceof ListType)
                return ListSetter;
            else if (column.type instanceof SetType)
                return SetSetter;
            else if (column.type instanceof MapType)
                return MapSetter;
            else if (column.type instanceof UserType)
                return UserTypeSetter;

            return ConstantSetter;
        }
        
        public Operation toOperation(ColumnMetadata column, Term keyOrIndex, FieldIdentifier field, Term value)
        {
            return toOperation.apply(column, keyOrIndex, field, value);
        }
    }

    private final Kind kind;
    private final ColumnMetadata receiver;
    private final ByteBuffer key;
    private final ByteBuffer field;
    private final TxnReferenceValue value;
    private final AbstractType<?> valueType;

    public TxnReferenceOperation(Kind kind, ColumnMetadata receiver, ByteBuffer key, ByteBuffer field, TxnReferenceValue value)
    {
        this.kind = kind;
        this.receiver = receiver;
        this.key = key;
        this.field = field;

        // We don't expect operators on clustering keys, but unwrap just in case.
        AbstractType<?> receiverType = receiver.type.unwrap();

        if (kind == TxnReferenceOperation.Kind.SetDiscarder && receiverType.isCollection() && (((CollectionType<?>) receiverType).kind == MAP))
        {
            // The value for a map subtraction is actually a set (see Operation.Substraction)
            this.valueType = SetType.getInstance(((MapType<?, ?>) receiverType).getKeysType(), true);
        }
        else if (kind == Kind.MapSetterByKey || kind == Kind.ListSetterByIndex)
        {
            this.valueType = ((CollectionType<?>) receiverType).valueComparator();
        }
        else if (kind == Kind.UserTypeSetterByField)
        {
            UserType userType = (UserType) receiverType;
            CellPath fieldPath = userType.cellPathForField(new FieldIdentifier(field));
            this.valueType = userType.fieldType(fieldPath);
        }
        else
        {
            this.valueType = receiverType;
        }

        this.value = value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnReferenceOperation that = (TxnReferenceOperation) o;
        return Objects.equals(receiver, that.receiver) 
               && kind == that.kind
               && Objects.equals(key, that.key)
               && Objects.equals(field, that.field)
               && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(receiver, kind, key, field, value);
    }

    @Override
    public String toString()
    {
        return receiver + " = " + value;
    }

    public ColumnMetadata receiver()
    {
        return receiver;
    }

    public void apply(TxnData data, DecoratedKey key, UpdateParameters up)
    {
        Operation operation = toOperation(data, up);
        operation.execute(key, up);
    }

    private Operation toOperation(TxnData data, UpdateParameters up)
    {
        FieldIdentifier fieldIdentifier = field == null ? null : new FieldIdentifier(field);
        Term valueTerm = toTerm(data, valueType, up.options.getProtocolVersion());
        Term keyorIndexTerm = key == null ? null : toTerm(key, valueType, up.options.getProtocolVersion());
        return kind.toOperation(receiver, keyorIndexTerm, fieldIdentifier, valueTerm);
    }

    private Term toTerm(TxnData data, AbstractType<?> receivingType, ProtocolVersion version)
    {
        ByteBuffer bytes = value.compute(data, receivingType);
        return toTerm(bytes, receivingType, version);
    }

    private Term toTerm(ByteBuffer bytes, AbstractType<?> receivingType, ProtocolVersion version)
    {
        if (receivingType.isCollection())
            return AccordSerializers.deserializeCqlCollectionAsTerm(bytes, receivingType, version);
        else if (receivingType.isUDT())
            return UserTypes.Value.fromSerialized(bytes, (UserType) receivingType);
        else if (receivingType.isTuple())
            return Tuples.Value.fromSerialized(bytes, (TupleType) receivingType);

        return new Constants.Value(bytes);
    }

    static final IVersionedSerializer<TxnReferenceOperation> serializer = new IVersionedSerializer<TxnReferenceOperation>()
    {
        @Override
        public void serialize(TxnReferenceOperation operation, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(operation.kind.id);
            columnMetadataSerializer.serialize(operation.receiver, out, version);
            TxnReferenceValue.serializer.serialize(operation.value, out, version);

            out.writeBoolean(operation.key != null);
            if (operation.key != null)
                ByteBufferUtil.writeWithVIntLength(operation.key, out);

            out.writeBoolean(operation.field != null);
            if (operation.field != null)
                ByteBufferUtil.writeWithVIntLength(operation.field, out);
        }

        @Override
        public TxnReferenceOperation deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.from(in.readByte());
            ColumnMetadata receiver = columnMetadataSerializer.deserialize(in, version);
            TxnReferenceValue value = TxnReferenceValue.serializer.deserialize(in, version);
            ByteBuffer key = in.readBoolean() ? ByteBufferUtil.readWithVIntLength(in) : null;
            ByteBuffer field = in.readBoolean() ? ByteBufferUtil.readWithVIntLength(in) : null;
            return new TxnReferenceOperation(kind, receiver, key, field, value);
        }

        @Override
        public long serializedSize(TxnReferenceOperation operation, int version)
        {
            long size = Byte.BYTES;
            size += columnMetadataSerializer.serializedSize(operation.receiver, version);
            size += TxnReferenceValue.serializer.serializedSize(operation.value, version);

            if (operation.key != null)
                size += ByteBufferUtil.serializedSizeWithVIntLength(operation.key);

            if (operation.field != null)
                size += ByteBufferUtil.serializedSizeWithVIntLength(operation.field);

            return size;
        }
    };
}
