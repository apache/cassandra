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
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * A simple UTF-8 string serializer for use with (primarily) collection serialization methods.
 */
public final class StringSerializer implements IVersionedSerializer<String>, UnversionedSerializer<String>
{
    public static final StringSerializer instance = new StringSerializer();

    @Override
    public void serialize(String str, DataOutputPlus out, int version) throws IOException
    {
        serialize(str, out);
    }

    @Override
    public void serialize(String str, DataOutputPlus out) throws IOException
    {
        out.writeUTF(str);
    }

    @Override
    public String deserialize(DataInputPlus in, int version) throws IOException
    {
        return deserialize(in);
    }

    @Override
    public String deserialize(DataInputPlus in) throws IOException
    {
        return in.readUTF();
    }

    @Override
    public long serializedSize(String str, int version)
    {
        return serializedSize(str);
    }

    @Override
    public long serializedSize(String str)
    {
        return TypeSizes.sizeof(str);
    }
}
