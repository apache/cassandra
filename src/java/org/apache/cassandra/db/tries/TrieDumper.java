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

import java.util.function.Function;

/**
 * Simple utility class for dumping the structure of a trie to string.
 */
class TrieDumper<T> implements TrieWalker<T, String>
{
    private final Function<T, String> contentToString;
    private final StringBuilder b = new StringBuilder();
    private int depth = -1;
    private boolean indented = true;

    TrieDumper(Function<T, String> contentToString)
    {
        this.contentToString = contentToString;
    }

    public void onNodeEntry(int incomingTransition, T content)
    {
        if (!indented)
        {
            for (int i = 0; i < depth; ++i)
                b.append("  ");
            indented = true;
        }

        ++depth;
        if (incomingTransition != -1)
            b.append(String.format("%02x", incomingTransition));

        if (content != null)
        {
            // Only go to a new line once a payload is reached
            indented = false;
            b.append(" -> ");
            b.append(contentToString.apply(content));
            b.append('\n');
        }
    }

    public void onNodeExit()
    {
        if (indented)
        {
            // We are backtracking without having printed content or meta. Although unexpected, this can legally happen
            // (e.g. if an intersection has resulted in an empty node).
            indented = false;
            b.append('\n');
        }
        --depth;
    }

    public String completion()
    {
        return b.toString();
    }
}
