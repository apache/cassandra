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

package org.apache.cassandra.cql3.functions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

import org.apache.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.cql3.functions.types.TupleType;
import org.apache.cassandra.cql3.functions.types.TupleValue;
import org.apache.cassandra.cql3.functions.types.UDTValue;
import org.apache.cassandra.cql3.functions.types.UserType;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JavaDriverUtils;

/**
 * Package private implementation of {@link UDFContext}
 */
public final class UDFContextImpl implements UDFContext
{
    private final Map<String, UDFDataType> byName = new HashMap<>();
    private final List<UDFDataType> argTypes;
    private final UDFDataType returnType;
    private final String keyspace;

    /*
        metadata can not be retrieved within the constructor as the keyspace itself may be being
        constructed and an NPE will result.
     */
    private final Supplier<KeyspaceMetadata> metadata;

    UDFContextImpl(List<ColumnIdentifier> argNames,
                   List<UDFDataType> argTypes,
                   UDFDataType returnType,
                   String keyspace)
    {
        for (int i = 0; i < argNames.size(); i++)
            byName.put(argNames.get(i).toString(), argTypes.get(i));
        this.argTypes = argTypes;
        this.returnType = returnType;
        this.keyspace = keyspace;
        this.metadata = Suppliers.memoize(() -> Schema.instance.getKeyspaceMetadata(keyspace));
    }

    public UDTValue newArgUDTValue(String argName)
    {
        return newUDTValue(getArgumentTypeByName(argName).toDataType());
    }

    public UDTValue newArgUDTValue(int argNum)
    {
        return newUDTValue(getArgumentTypeByIndex(argNum).toDataType());
    }

    public UDTValue newReturnUDTValue()
    {
        return newUDTValue(returnType.toDataType());
    }

    public UDTValue newUDTValue(String udtName)
    {
        Optional<org.apache.cassandra.db.marshal.UserType> udtType = metadata.get().types.get(ByteBufferUtil.bytes(udtName));
        DataType dataType = JavaDriverUtils.driverType(udtType.orElseThrow(
                () -> new IllegalArgumentException("No UDT named " + udtName + " in keyspace " + keyspace)
        ));
        return newUDTValue(dataType);
    }

    public TupleValue newArgTupleValue(String argName)
    {
        return newTupleValue(getArgumentTypeByName(argName).toDataType());
    }

    public TupleValue newArgTupleValue(int argNum)
    {
        return newTupleValue(getArgumentTypeByIndex(argNum).toDataType());
    }

    public TupleValue newReturnTupleValue()
    {
        return newTupleValue(returnType.toDataType());
    }

    public TupleValue newTupleValue(String cqlDefinition)
    {
        AbstractType<?> abstractType = CQLTypeParser.parse(keyspace, cqlDefinition, metadata.get().types);
        DataType dataType = JavaDriverUtils.driverType(abstractType);
        return newTupleValue(dataType);
    }

    private static UDTValue newUDTValue(DataType dataType)
    {
        if (!(dataType instanceof UserType))
            throw new IllegalStateException("Function argument is not a UDT but a " + dataType.getName());
        UserType userType = (UserType) dataType;
        return userType.newValue();
    }

    private static TupleValue newTupleValue(DataType dataType)
    {
        if (!(dataType instanceof TupleType))
            throw new IllegalStateException("Function argument is not a tuple type but a " + dataType.getName());
        TupleType tupleType = (TupleType) dataType;
        return tupleType.newValue();
    }

    private UDFDataType getArgumentTypeByIndex(int index)
    {
        if (index < 0 || index >= argTypes.size())
            throw new IllegalArgumentException("Function does not declare an argument with index " + index);
        return argTypes.get(index);
    }

    private UDFDataType getArgumentTypeByName(String argName)
    {
        UDFDataType arg = byName.get(argName);
        if (arg == null)
            throw new IllegalArgumentException("Function does not declare an argument named '" + argName + '\'');
        return arg;
    }
}
