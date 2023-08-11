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

import org.agrona.DirectBuffer;

/**
 * A class for dumping the structure of a trie to a graphviz/dot representation for making trie graphs.
 */
class TrieToMermaid<T> extends TriePathReconstructor implements Trie.Walker<T, String>
{
    private final StringBuilder b;
    private final Function<T, String> contentToString;
    private final Function<Integer, String> transitionToString;
    private final boolean useMultiByte;
    private int prevPos;
    private int currNodeTextPos;

    public TrieToMermaid(Function<T, String> contentToString,
                         Function<Integer, String> transitionToString,
                         boolean useMultiByte)
    {
        this.contentToString = contentToString;
        this.transitionToString = transitionToString;
        this.useMultiByte = useMultiByte;
        this.b = new StringBuilder();
        b.append("graph TD");
        newLineAndIndent();
        addNodeDefinition(nodeString(0));
        newLineAndIndent();
        b.append("style " + nodeString(0) + " fill:darkgrey");
    }

    @Override
    public void resetPathLength(int newLength)
    {
        super.resetPathLength(newLength);
        prevPos = newLength;
    }

    private void newLineAndIndent()
    {
        b.append('\n');
        for (int i = 0; i < prevPos + 1; ++i)
            b.append("  ");
    }

    @Override
    public void addPathByte(int nextByte)
    {
        newLineAndIndent();
        super.addPathByte(nextByte);
        b.append(nodeString(prevPos));
        String newNode = nodeString(keyPos);
        b.append(" --\"");
        for (int i = prevPos; i < keyPos - 1; ++i)
            b.append(transitionToString.apply(keyBytes[i] & 0xFF));
        b.append(transitionToString.apply(nextByte));
        b.append("\"--> ");
        addNodeDefinition(newNode);
    }

    private void addNodeDefinition(String newNode)
    {
        prevPos = keyPos;
        currNodeTextPos = b.length();
        b.append(String.format("%s(( ))", newNode));
    }

    private String nodeString(int keyPos)
    {
        StringBuilder b = new StringBuilder();
        b.append("Node_");
        for (int i = 0; i < keyPos; ++i)
            b.append(transitionToString.apply(keyBytes[i] & 0xFF));
        return b.toString();
    }

    @Override
    public void addPathBytes(DirectBuffer buffer, int pos, int count)
    {
        if (useMultiByte)
        {
            super.addPathBytes(buffer, pos, count);
        }
        else
        {
            for (int i = 0; i < count; ++i)
                addPathByte(buffer.getByte(pos + i) & 0xFF);
        }
    }

    @Override
    public void content(T content)
    {
        b.replace(currNodeTextPos, b.length(), String.format("%s(((%s)))", nodeString(keyPos), contentToString.apply(content)));
    }

    @Override
    public String complete()
    {
        b.append("\n");
        return b.toString();
    }
}
