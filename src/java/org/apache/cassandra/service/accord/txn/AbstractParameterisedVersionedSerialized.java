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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import accord.utils.Invariants;
import org.apache.cassandra.service.accord.serializers.Version;

/**
 * Item that is serialized by default
 */
@NotThreadSafe
public abstract class AbstractParameterisedVersionedSerialized<T, P> extends AbstractSerialized<T>
{
    protected AbstractParameterisedVersionedSerialized(@Nullable ByteBuffer latestVersionBytes)
    {
        super(latestVersionBytes);
    }

    protected abstract ByteBuffer serialize(T value, P param, Version version);
    protected abstract ByteBuffer reserialize(ByteBuffer bytes, P param, Version srcVersion, Version trgVersion);
    protected abstract T deserialize(P param, ByteBuffer bytes, Version version);

    @Nullable
    protected T deserialize(P param)
    {
        T result = memoized;
        if (result == null && latestVersionBytes != null)
            memoized = result = deserialize(param, latestVersionBytes, Version.LATEST);
        return result;
    }

    @Nonnull
    protected ByteBuffer bytes(P param, Version target)
    {
        Invariants.nonNull(latestVersionBytes);
        if (Version.LATEST == target)
            return latestVersionBytes;
        return reserialize(latestVersionBytes, param, Version.LATEST, target);
    }
}
