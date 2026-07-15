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
package org.apache.cassandra.service.accord;

import accord.local.SafeState;

import org.apache.cassandra.service.accord.AccordCacheEntry.LockMode;

public interface AccordSafeState<K, V, S extends SafeState<V> & AccordSafeState<K, V, S>>
{
    AccordCacheEntry<K, V, S> global();
    void postExecute(AccordTask<?> owner);
    void preExecute(AccordTask<?> owner, LockMode lockMode);

    static AccordCacheEntry<?, ?, ?> global(SafeState<?> safeState)
    {
        return safeState.getClass() == AccordSafeCommand.class ? ((AccordSafeCommand) safeState).global()
                                                               : ((AccordSafeCommandsForKey) safeState).global();
    }

    static void postExecute(SafeState<?> safeState, AccordTask<?> owner)
    {
        if (safeState.getClass() == AccordSafeCommand.class) ((AccordSafeCommand) safeState).postExecute(owner);
        else ((AccordSafeCommandsForKey) safeState).postExecute(owner);
    }

    static void preExecute(SafeState<?> safeState, AccordTask<?> owner, LockMode lockMode)
    {
        if (safeState.getClass() == AccordSafeCommand.class) ((AccordSafeCommand) safeState).preExecute(owner, lockMode);
        else ((AccordSafeCommandsForKey) safeState).preExecute(owner, lockMode);
    }
}
