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

package org.apache.cassandra.utils;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class Int64Serializer implements IVersionedSerializer<Long>
{
    public static final Int64Serializer serializer = new Int64Serializer();

    @Override
    public void serialize(Long t, DataOutputPlus out, int version) throws IOException
    {
        out.writeLong(t);
    }

    @Override
    public Long deserialize(DataInputPlus in, int version) throws IOException
    {
        return in.readLong();
    }

    @Override
    public long serializedSize(Long t, int version)
    {
        return TypeSizes.sizeof(t);
    }
}
