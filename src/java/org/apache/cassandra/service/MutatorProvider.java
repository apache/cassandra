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

package org.apache.cassandra.service;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_MUTATOR_CLASS;

/**
 * Provides an instance of {@link Mutator} that facilitates mutation writes for standard mutations, unlogged batches,
 * counters and paxos commits (LWT)s.
 * <br/>
 * An implementation may choose to fallback to the default implementation ({@link StorageProxy.DefaultMutator})
 * obtained via {@link #getDefaultMutator()}.
 */
public abstract class MutatorProvider
{
    static final Mutator instance = getCustomOrDefault();

    public static Mutator getCustomOrDefault()
    {
        if (CUSTOM_MUTATOR_CLASS.isPresent())
        {
            return FBUtilities.construct(CUSTOM_MUTATOR_CLASS.getString(),
                                         "custom mutator class (set with " + CUSTOM_MUTATOR_CLASS.getKey() + ")");
        }
        else
        {
            return getDefaultMutator();
        }
    }

    public static Mutator getDefaultMutator()
    {
        return new StorageProxy.DefaultMutator();
    }
}
