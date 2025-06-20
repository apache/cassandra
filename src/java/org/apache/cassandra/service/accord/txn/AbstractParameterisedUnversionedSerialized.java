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

import java.nio.ByteBuffer;
import javax.annotation.Nullable;

import org.apache.cassandra.io.ParameterisedUnversionedSerializer;

public abstract class AbstractParameterisedUnversionedSerialized<T, P> extends AbstractSerialized<T>
{
    public AbstractParameterisedUnversionedSerialized(@Nullable ByteBuffer latestVersionBytes)
    {
        super(latestVersionBytes);
    }

    protected abstract ParameterisedUnversionedSerializer<T, P> serializer();

    protected T deserialize(P param)
    {
        T result = memoized;
        if (result == null && latestVersionBytes != null)
            memoized = result = serializer().deserializeUnchecked(param, latestVersionBytes);
        return result;
    }
}
