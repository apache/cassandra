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

package org.apache.cassandra.concurrent;

import java.nio.ByteBuffer;

/**
 * An extension of {@link DebuggableTask} that exposes query metadata for executor queue
 * prioritization and concurrency throttling (e.g., isolating concurrent LWTs per partition).
 */
public interface PrioritizableTask extends DebuggableTask
{
    boolean isLWT();

    /**
     * @return the serialized routing key of the target partition, or {@code null} if unknown or unapplicable.
     */
    ByteBuffer partitionKey();

    /**
     * Unwraps and returns the underlying {@link PrioritizableTask} from a given {@link Runnable},
     * resolving through any {@link FutureTask} wrapping layers.
     *
     * @param task the task to inspect
     * @return the resolved {@link PrioritizableTask}, or {@code null} if not prioritizable
     */
    static PrioritizableTask get(Runnable task)
    {
        if (task instanceof PrioritizableTask)
            return (PrioritizableTask) task;

        if (task instanceof FutureTask)
        {
            DebuggableTask debuggable = ((FutureTask<?>) task).debuggableTask();
            if (debuggable instanceof PrioritizableTask)
                return (PrioritizableTask) debuggable;
        }

        return null;
    }
}