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
package org.apache.cassandra.service;

import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.io.DataInput;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

/**
 * Abstract the conditions and updates for a CAS operation.
 */
public interface CASRequest
{
    public static enum Type
    {
        THRIFT, CQL
    }

    Map<Type, IVersionedSerializer<CASRequest>> serializerMap = new EnumMap<Type, IVersionedSerializer<CASRequest>>(Type.class)
    {{
            put(Type.CQL, CQL3CasRequest.serializer);
            put(Type.THRIFT, ThriftCASRequest.serializer);
    }};

    public static final IVersionedSerializer<CASRequest> serializer = new IVersionedSerializer<CASRequest>()
    {
        @Override
        public void serialize(CASRequest request, DataOutputPlus out, int version) throws IOException
        {
            Type type = request.getType();
            out.writeInt(type.ordinal());
            serializerMap.get(type).serialize(request, out, version);

        }

        @Override
        public CASRequest deserialize(DataInput in, int version) throws IOException
        {
            Type type = Type.values()[in.readInt()];
            return serializerMap.get(type).deserialize(in, version);
        }

        @Override
        public long serializedSize(CASRequest request, int version)
        {
            Type type = request.getType();
            return 4 + serializerMap.get(type).serializedSize(request, version);
        }
    };

    /**
     * The filter to use to fetch the value to compare for the CAS.
     */
    public IDiskAtomFilter readFilter();

    /**
     * Returns whether the provided CF, that represents the values fetched using the
     * readFilter(), match the CAS conditions this object stands for.
     */
    public boolean appliesTo(ColumnFamily current) throws InvalidRequestException;

    /**
     * The updates to perform of a CAS success. The values fetched using the readFilter()
     * are passed as argument.
     */
    public ColumnFamily makeUpdates(ColumnFamily current) throws InvalidRequestException;

    public Type getType();
}
