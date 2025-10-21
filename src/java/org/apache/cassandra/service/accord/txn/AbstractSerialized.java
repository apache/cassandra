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
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import accord.utils.Invariants;

/**
 * Item that is serialized by default
 */
@NotThreadSafe
public abstract class AbstractSerialized<T>
{
    protected @Nullable final ByteBuffer latestVersionBytes;
    protected transient @Nullable T memoized = null;

    public AbstractSerialized(@Nullable ByteBuffer latestVersionBytes)
    {
        this.latestVersionBytes = latestVersionBytes;
    }

    public abstract long estimatedSizeOnHeap();

    protected boolean isNull()
    {
        return latestVersionBytes == null;
    }

    public void unmemoize()
    {
        memoized = null;
    }

    @Nullable
    protected ByteBuffer unsafeBytes()
    {
        return latestVersionBytes;
    }

    @Nonnull
    protected ByteBuffer bytes()
    {
        Invariants.nonNull(latestVersionBytes);
        return latestVersionBytes;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || (o.getClass() != getClass())) return false;

        AbstractSerialized<?> that = (AbstractSerialized<?>) o;
        return Objects.equals(latestVersionBytes, that.latestVersionBytes);
    }

    @Override
    public int hashCode()
    {
        return latestVersionBytes != null ? latestVersionBytes.hashCode() : 0;
    }
}
