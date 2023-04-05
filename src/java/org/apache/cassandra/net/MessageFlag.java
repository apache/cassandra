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
package org.apache.cassandra.net;

import static java.lang.Math.max;

/**
 * Binary message flags to be passed as {@code flags} field of {@link Message}.
 */
public enum MessageFlag
{
    /** a failure response should be sent back in case of failure */
    CALL_BACK_ON_FAILURE (0),
    /** track repaired data - see CASSANDRA-14145 */
    TRACK_REPAIRED_DATA  (1),
    /** allow creating warnings or aborting queries based off query - see CASSANDRA-16850 */
    TRACK_WARNINGS(2);

    private final int id;

    MessageFlag(int id)
    {
        this.id = id;
    }

    /**
     * @return {@code true} if the flag is present in provided flags, {@code false} otherwise
     */
    boolean isIn(int flags)
    {
        return (flags & (1 << id)) != 0;
    }

    /**
     * @return new flags value with this flag added
     */
    int addTo(int flags)
    {
        return flags | (1 << id);
    }

    private static final MessageFlag[] idToFlagMap;
    static
    {
        MessageFlag[] flags = values();

        int max = -1;
        for (MessageFlag flag : flags)
            max = max(flag.id, max);

        MessageFlag[] idMap = new MessageFlag[max + 1];
        for (MessageFlag flag : flags)
        {
            if (idMap[flag.id] != null)
                throw new RuntimeException("Two MessageFlag-s that map to the same id: " + flag.id);
            idMap[flag.id] = flag;
        }
        idToFlagMap = idMap;
    }

    @SuppressWarnings("unused")
    MessageFlag lookUpById(int id)
    {
        if (id < 0)
            throw new IllegalArgumentException("MessageFlag id must be non-negative (got " + id + ')');

        return id < idToFlagMap.length ? idToFlagMap[id] : null;
    }
}

