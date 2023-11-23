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

package org.apache.cassandra.tcm.extensions;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.Version;

public class StringValue extends AbstractExtensionValue<String>
{
    public static StringValue create(String s)
    {
        StringValue v = new StringValue();
        v.setValue(s);
        return v;
    }

    @Override
    void serializeInternal(DataOutputPlus out, Version version) throws IOException
    {
        out.writeUTF(getValue());
    }

    @Override
    void deserializeInternal(DataInputPlus in, Version version) throws IOException
    {
        setValue(in.readUTF());
    }

    @Override
    long serializedSizeInternal(Version v)
    {
        return TypeSizes.sizeof(getValue());
    }
}
