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
package org.apache.cassandra.utils.obs;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.utils.concurrent.Ref;

public interface IBitSet extends Closeable
{
    public long capacity();

    /**
     * Returns true or false for the specified bit index. The index should be
     * less than the capacity.
     */
    public boolean get(long index);

    /**
     * Sets the bit at the specified index. The index should be less than the
     * capacity.
     */
    public void set(long index);

    /**
     * clears the bit. The index should be less than the capacity.
     */
    public void clear(long index);

    public void serialize(DataOutput out) throws IOException;

    public long serializedSize();

    public void clear();

    public void close();

    /**
     * Returns the amount of memory in bytes used off heap.
     * @return the amount of memory in bytes used off heap
     */
    public long offHeapSize();

    public void addTo(Ref.IdentityCollection identities);
}
