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

import java.util.function.ToLongFunction;
import javax.annotation.Nullable;

import org.apache.cassandra.service.ClientState;

/**
 * A guardrail based on numeric threshold(s).
 *
 * <p>A {@link MaxThreshold} guardrail defines (up to) 2 thresholds, one at which a warning is issued, and a higher one
 * at which the operation is aborted with an exception. Only one of those thresholds can be activated if desired.
 *
 * <p>This guardrail only handles guarding positive values.
 */
public class MaxThreshold extends Threshold
{

    /**
     * Creates a new threshold guardrail.
     *
     * @param name            the identifying name of the guardrail
     * @param warnThreshold   a {@link ClientState}-based provider of the value above which a warning should be triggered.
     * @param failThreshold   a {@link ClientState}-based provider of the value above which the operation should be aborted.
     * @param messageProvider a function to generate the warning or error message if the guardrail is triggered
     */
    public MaxThreshold(String name,
                        ToLongFunction<ClientState> warnThreshold,
                        ToLongFunction<ClientState> failThreshold,
                        Threshold.ErrorMessageProvider messageProvider)
    {
        super(name, warnThreshold, failThreshold, messageProvider);
        this.warnThreshold = warnThreshold;
        this.failThreshold = failThreshold;
    }

    protected boolean compare(long value, long threshold)
    {
        return value > threshold;
    }

    protected long failValue(ClientState state)
    {
        long failValue = failThreshold.applyAsLong(state);
        return failValue <= 0 ? Long.MAX_VALUE : failValue;
    }

    protected long warnValue(ClientState state)
    {
        long warnValue = warnThreshold.applyAsLong(state);
        return warnValue <= 0 ? Long.MAX_VALUE : warnValue;
    }

    public boolean triggersOn(long value, @Nullable ClientState state)
    {
        return enabled(state) && (value > Math.min(failValue(state), warnValue(state)));
    }

    public boolean warnsOn(long value, @Nullable ClientState state)
    {
        return enabled(state) && (value > warnValue(state) && value <= failValue(state));
    }

    public boolean failsOn(long value, @Nullable ClientState state)
    {
        return enabled(state) && (value > failValue(state));
    }

}
