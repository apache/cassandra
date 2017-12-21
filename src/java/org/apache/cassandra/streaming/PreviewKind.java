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

package org.apache.cassandra.streaming;


import java.util.UUID;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import org.apache.cassandra.io.sstable.format.SSTableReader;

public enum PreviewKind
{
    NONE(0, null),
    ALL(1, Predicates.alwaysTrue()),
    UNREPAIRED(2, Predicates.not(SSTableReader::isRepaired)),
    REPAIRED(3, SSTableReader::isRepaired);

    private final int serializationVal;
    private final Predicate<SSTableReader> streamingPredicate;

    PreviewKind(int serializationVal, Predicate<SSTableReader> streamingPredicate)
    {
        assert ordinal() == serializationVal;
        this.serializationVal = serializationVal;
        this.streamingPredicate = streamingPredicate;
    }

    public int getSerializationVal()
    {
        return serializationVal;
    }

    public static PreviewKind deserialize(int serializationVal)
    {
        return values()[serializationVal];
    }

    public Predicate<SSTableReader> getStreamingPredicate()
    {
        return streamingPredicate;
    }

    public boolean isPreview()
    {
        return this != NONE;
    }

    public String logPrefix()
    {
        return isPreview() ? "preview repair" : "repair";
    }

    public String logPrefix(UUID sessionId)
    {
        return '[' + logPrefix() + " #" + sessionId.toString() + ']';
    }

}
