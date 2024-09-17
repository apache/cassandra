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

import java.util.Collection;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class StreamReceivedOutOfTokenRangeException extends RuntimeException
{
    private final Collection<Range<Token>> ownedRanges;
    private final DecoratedKey key;
    private final String filename;

    public StreamReceivedOutOfTokenRangeException(Collection<Range<Token>> ownedRanges,
                                                  DecoratedKey key,
                                                  String filename)
    {
        this.ownedRanges = ownedRanges;
        this.key = key;
        this.filename = filename;
    }

    public String getMessage()
    {
        return String.format("Received stream for sstable %s containing key %s outside of owned ranges %s ",
                             filename,
                             key,
                             ownedRanges);
    }
}
