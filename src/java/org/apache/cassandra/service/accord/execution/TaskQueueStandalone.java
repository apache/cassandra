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

import accord.utils.TinyEnumSet;

final class TaskQueueStandalone<T extends Task> extends TaskQueue<T>
{
    TaskQueueStandalone(int states)
    {
        super(states);
    }

    TaskQueueStandalone(Task.State state)
    {
        super(TinyEnumSet.encode(state));
    }

    void enqueue(T enqueue)
    {
        enqueue.setQueue(this);
        enqueueSingle(enqueue);
    }

    void unqueue(T unqueue)
    {
        unqueue.unsetQueue(this);
        removeNode(unqueue);
    }

    T peek()
    {
        return peekSingle();
    }

    T poll()
    {
        return pollSingle();
    }

    boolean isEmpty()
    {
        return isEmptySingle();
    }
}
