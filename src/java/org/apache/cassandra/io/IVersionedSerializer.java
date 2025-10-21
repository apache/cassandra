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
package org.apache.cassandra.io;

import java.io.IOException;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public interface IVersionedSerializer<T> extends IVersionedAsymmetricSerializer<T, T>
{
    static <T> IVersionedSerializer<T> from(UnversionedSerializer<T> delegate)
    {
        return new IVersionedSerializer<T>()
        {
            @Override
            public void serialize(T t, DataOutputPlus out, int version) throws IOException
            {
                delegate.serialize(t, out);
            }

            @Override
            public T deserialize(DataInputPlus in, int version) throws IOException
            {
                return delegate.deserialize(in);
            }

            @Override
            public long serializedSize(T t, int version)
            {
                return delegate.serializedSize(t);
            }
        };
    }
}
