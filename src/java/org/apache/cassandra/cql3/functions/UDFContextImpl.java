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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Package private implementation of {@link UDFContext}
 */
public final class UDFContextImpl implements UDFContext
{
    private final KeyspaceMetadata keyspaceMetadata;
    private final Map<String, TypeCodec<Object>> byName = new HashMap<>();
    private final TypeCodec<Object>[] argCodecs;
    private final TypeCodec<Object> returnCodec;

    UDFContextImpl(List<ColumnIdentifier> argNames, TypeCodec<Object>[] argCodecs, TypeCodec<Object> returnCodec,
                   KeyspaceMetadata keyspaceMetadata)
    {
        for (int i = 0; i < argNames.size(); i++)
            byName.put(argNames.get(i).toString(), argCodecs[i]);
        this.argCodecs = argCodecs;
        this.returnCodec = returnCodec;
        this.keyspaceMetadata = keyspaceMetadata;
    }

    public UDTValue newArgUDTValue(String argName)
    {
        return newUDTValue(codecFor(argName));
    }

    public UDTValue newArgUDTValue(int argNum)
    {
        return newUDTValue(codecFor(argNum));
    }

    public UDTValue newReturnUDTValue()
    {
        return newUDTValue(returnCodec);
    }

    public UDTValue newUDTValue(String udtName)
    {
        Optional<org.apache.cassandra.db.marshal.UserType> udtType = keyspaceMetadata.types.get(ByteBufferUtil.bytes(udtName));
        DataType dataType = UDHelper.driverType(udtType.orElseThrow(
                () -> new IllegalArgumentException("No UDT named " + udtName + " in keyspace " + keyspaceMetadata.name)
            ));
        return newUDTValue(dataType);
    }

    public TupleValue newArgTupleValue(String argName)
    {
        return newTupleValue(codecFor(argName));
    }

    public TupleValue newArgTupleValue(int argNum)
    {
        return newTupleValue(codecFor(argNum));
    }

    public TupleValue newReturnTupleValue()
    {
        return newTupleValue(returnCodec);
    }

    public TupleValue newTupleValue(String cqlDefinition)
    {
        AbstractType<?> abstractType = CQLTypeParser.parse(keyspaceMetadata.name, cqlDefinition, keyspaceMetadata.types);
        DataType dataType = UDHelper.driverType(abstractType);
        return newTupleValue(dataType);
    }

    private TypeCodec<Object> codecFor(int argNum)
    {
        if (argNum < 0 || argNum >= argCodecs.length)
            throw new IllegalArgumentException("Function does not declare an argument with index " + argNum);
        return argCodecs[argNum];
    }

    private TypeCodec<Object> codecFor(String argName)
    {
        TypeCodec<Object> codec = byName.get(argName);
        if (codec == null)
            throw new IllegalArgumentException("Function does not declare an argument named '" + argName + '\'');
        return codec;
    }

    private static UDTValue newUDTValue(TypeCodec<Object> codec)
    {
        DataType dataType = codec.getCqlType();
        return newUDTValue(dataType);
    }

    private static UDTValue newUDTValue(DataType dataType)
    {
        if (!(dataType instanceof UserType))
            throw new IllegalStateException("Function argument is not a UDT but a " + dataType.getName());
        UserType userType = (UserType) dataType;
        return userType.newValue();
    }

    private static TupleValue newTupleValue(TypeCodec<Object> codec)
    {
        DataType dataType = codec.getCqlType();
        return newTupleValue(dataType);
    }

    private static TupleValue newTupleValue(DataType dataType)
    {
        if (!(dataType instanceof TupleType))
            throw new IllegalStateException("Function argument is not a tuple type but a " + dataType.getName());
        TupleType tupleType = (TupleType) dataType;
        return tupleType.newValue();
    }
}
