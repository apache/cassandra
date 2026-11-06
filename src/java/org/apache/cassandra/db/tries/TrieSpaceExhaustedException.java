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

package org.apache.cassandra.db.tries;

/**
 * Because we use buffers and 32-bit pointers, the trie cannot grow over 2GB of size. This exception is thrown if
 * a trie operation needs it to grow over that limit.
 * <p>
 * To avoid this problem, users should query {@link InMemoryTrie#reachedAllocatedSizeThreshold} from time to time. If
 * the call returns true, they should switch to a new trie (e.g. by flushing a memtable) as soon as possible. The
 * threshold is configurable, and is set by default to 10% under the 2GB limit to give ample time for the switch to
 * happen.
 */
public class TrieSpaceExhaustedException extends Exception
{
    public TrieSpaceExhaustedException()
    {
        super("The hard 2GB limit on trie size has been exceeded");
    }
}
