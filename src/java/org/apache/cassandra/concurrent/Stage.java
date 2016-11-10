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

import java.util.Arrays;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public enum Stage
{
    READ,
    MUTATION,
    COUNTER_MUTATION,
    GOSSIP,
    REQUEST_RESPONSE,
    ANTI_ENTROPY,
    MIGRATION,
    MISC,
    TRACING,
    INTERNAL_RESPONSE,
    READ_REPAIR;

    public static Iterable<Stage> jmxEnabledStages()
    {
        return Iterables.filter(Arrays.asList(values()), new Predicate<Stage>()
        {
            public boolean apply(Stage stage)
            {
                return stage != TRACING;
            }
        });
    }

    public String getJmxType()
    {
        switch (this)
        {
            case ANTI_ENTROPY:
            case GOSSIP:
            case MIGRATION:
            case MISC:
            case TRACING:
            case INTERNAL_RESPONSE:
                return "internal";
            case MUTATION:
            case COUNTER_MUTATION:
            case READ:
            case REQUEST_RESPONSE:
            case READ_REPAIR:
                return "request";
            default:
                throw new AssertionError("Unknown stage " + this);
        }
    }

    public String getJmxName()
    {
        String name = "";
        for (String word : toString().split("_"))
        {
            name += word.substring(0, 1) + word.substring(1).toLowerCase();
        }
        return name + "Stage";
    }
}
