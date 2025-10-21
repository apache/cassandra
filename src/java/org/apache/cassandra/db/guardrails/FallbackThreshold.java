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

package org.apache.cassandra.db.guardrails;

import javax.annotation.Nullable;

import org.apache.cassandra.service.ClientState;


/**
 * Guardrail type that defines a fallback guardail in case the primare guardrail is either not enabled or
 * is valid.
 */
public class FallbackThreshold<T extends Threshold> extends Threshold
{
    private final T primary;
    private final T fallback;

    public FallbackThreshold(T primary, T fallback)
    {
        super(primary.name, primary.reason, primary.warnThreshold, primary.failThreshold, primary.messageProvider);
        this.primary = primary;
        this.fallback = fallback;
    }

    @Override
    public void guard(long value, String what, boolean containsUserData, @Nullable ClientState state)
    {
        if (primary.enabled(state))
        {
            primary.guard(value, what, containsUserData, state);
        }
        else
        {
            fallback.guard(value, what, containsUserData, state);
        }
    }

    @Override
    protected boolean compare(long value, long threshold)
    {
        throw new UnsupportedOperationException("Guardrail fallback wrapper does not support calling this method");
    }

    @Override
    protected long failValue(ClientState state)
    {
        throw new UnsupportedOperationException("Guardrail fallback wrapper does not support calling this method");
    }

    @Override
    protected long warnValue(ClientState state)
    {
        throw new UnsupportedOperationException("Guardrail fallback wrapper does not support calling this method");
    }
}
