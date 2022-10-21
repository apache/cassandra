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

package org.apache.cassandra.service.accord.db;

import java.io.IOException;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.Data;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Update;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

public abstract class AccordQuery implements Query
{
    public static final AccordQuery ALL = new AccordQuery()
    {
        @Override
        public Result compute(Data data, @Nullable Read read, @Nullable Update update)
        {
            return data != null ? (AccordData) data : new AccordData();
        }
    };

    public static final AccordQuery NONE = new AccordQuery()
    {
        @Override
        public Result compute(Data data, @Nullable Read read, @Nullable Update update)
        {
            return new AccordData();
        }
    };

    private static final long SIZE = ObjectSizes.measure(ALL);

    private AccordQuery() {}

    public long estimatedSizeOnHeap()
    {
        return SIZE;
    }

    public static final IVersionedSerializer<AccordQuery> serializer = new IVersionedSerializer<AccordQuery>()
    {
        @Override
        public void serialize(AccordQuery query, DataOutputPlus out, int version) throws IOException
        {
            Preconditions.checkArgument(query == ALL || query == NONE);
            out.writeBoolean(query == ALL);
        }

        @Override
        public AccordQuery deserialize(DataInputPlus in, int version) throws IOException
        {
            return in.readBoolean() ? ALL : NONE;
        }

        @Override
        public long serializedSize(AccordQuery query, int version)
        {
            Preconditions.checkArgument(query == ALL || query == NONE);
            return TypeSizes.sizeof(query == ALL);
        }
    };
}
