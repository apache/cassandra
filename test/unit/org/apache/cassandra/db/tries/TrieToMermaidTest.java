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

import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;

public class TrieToMermaidTest
{
    @Test
    public void testToMermaidContent() throws Exception
    {
        InMemoryTrie<String> trie = new InMemoryTrie<>(BufferType.OFF_HEAP);
        // This was used as a basis the graphs in BTIFormat.md
        String s = "a allow an and any are as node of on the this to trie types with without";
        s = s.toLowerCase();
        for (String word : s.split("[^a-z]+"))
            trie.putRecursive(InMemoryTrieTestBase.comparable(word), word, (x, y) -> y);

        System.out.println(trie.process(new TrieToMermaid(Object::toString,
                                                      x -> Character.toString((char) ((int) x)),
                                                      false)));
    }
}
