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
package org.apache.cassandra.io;

/**
 * Classifies an operation's eligibility for a direct-IO (O_DIRECT) data path, encoding both
 * the answer and the rationale class. Consumers maintain their own per-operation classification
 * and apply this alongside their own gates (e.g. compression, configuration mode);
 * {@link #SUPPORTED} is necessary but not sufficient.
 */
public enum DirectIoSupport
{
    /** Eligible for the direct-IO data path. */
    SUPPORTED,

    /**
     * The direct-IO path is mechanically incompatible with this operation. Removing this
     * exclusion requires code changes, not policy.
     */
    UNSUPPORTED_CORRECTNESS,

    /**
     * Direct IO would work, but is deliberately disabled for performance or cache-residency
     * reasons. Removing this exclusion requires re-evaluating the policy, not code changes.
     */
    UNSUPPORTED_POLICY,

    /**
     * The operation does not exercise this data path (e.g. a sentinel, or an op that does not
     * read or write through the consuming component). The gate is moot.
     */
    NOT_APPLICABLE;

    public boolean isSupported()
    {
        return this == SUPPORTED;
    }
}
