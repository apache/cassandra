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

package org.apache.cassandra.service.accord.execution;

final class AccordCacheEntryMiniQueue
{
    final SafeTask<?> lockedBy;
    final SafeTask<?> next;

    public AccordCacheEntryMiniQueue(SafeTask<?> lockedBy, SafeTask<?> next)
    {
        this.lockedBy = lockedBy;
        this.next = next;
    }

    /** whether {@code task} occupies a queue position, which for a lock holder means it locked with HOLD_QUEUE */
    boolean contains(AccordCacheEntry<?, ?, ?> owner, SafeTask<?> task)
    {
        return task == next || (task == lockedBy && owner.isLockedHoldingQueue());
    }

    /** the one member of a mini queue that may run: the holder if it kept its position, else the other claim */
    SafeTask<?> head(AccordCacheEntry<?, ?, ?> owner)
    {
        return owner.isLockedHoldingQueue() ? lockedBy : next;
    }
}
