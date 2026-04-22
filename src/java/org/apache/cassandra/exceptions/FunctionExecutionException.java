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
package org.apache.cassandra.exceptions;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.StringSerializer;

public class FunctionExecutionException extends RequestExecutionException
{
    public final FunctionName functionName;
    public final List<String> argTypes;
    public final String detail;

    public static FunctionExecutionException create(Function function, Throwable cause)
    {
        List<String> cqlTypes = AbstractType.asCQLTypeStringList(function.argTypes());
        FunctionExecutionException fee = new FunctionExecutionException(function.name(), cqlTypes, cause.toString());
        fee.initCause(cause);
        return fee;
    }

    public static FunctionExecutionException create(FunctionName functionName, List<String> argTypes, String detail)
    {
        String msg = "execution of '" + functionName + argTypes + "' failed: " + detail;
        return new FunctionExecutionException(functionName, argTypes, msg);
    }

    public FunctionExecutionException(FunctionName functionName, List<String> argTypes, String msg)
    {
        super(ExceptionCode.FUNCTION_FAILURE, msg);
        this.functionName = functionName;
        this.argTypes = argTypes;
        this.detail = msg;
    }

    @Override
    protected void serializeSpecificFields(DataOutputPlus out, int version) throws IOException
    {
        out.writeBoolean(functionName.keyspace != null);
        if (functionName.keyspace != null)
            out.writeUTF(functionName.keyspace);
        out.writeUTF(functionName.name);
        CollectionSerializers.serializeList(argTypes, out, version, StringSerializer.instance);
        out.writeUTF(detail);
    }

    @Override
    protected long serializedSizeSpecificFields(int version)
    {
        long size = TypeSizes.BOOL_SIZE; // keyspace present flag
        if (functionName.keyspace != null)
            size += TypeSizes.sizeof(functionName.keyspace);
        size += TypeSizes.sizeof(functionName.name);
        size += CollectionSerializers.serializedListSize(argTypes, version, StringSerializer.instance);
        size += TypeSizes.sizeof(detail);
        return size;
    }

    static FunctionExecutionException deserializeFields(String message, DataInputPlus in, int version) throws IOException
    {
        String keyspace = in.readBoolean() ? in.readUTF() : null;
        String name = in.readUTF();
        List<String> argTypes = CollectionSerializers.deserializeList(in, version, StringSerializer.instance);
        String detail = in.readUTF();
        return new FunctionExecutionException(new FunctionName(keyspace, name), argTypes, detail);
    }

    @Override
    public CassandraExceptionCode getCassandraExceptionCode()
    {
        return CassandraExceptionCode.FUNCTION_FAILURE;
    }
}
