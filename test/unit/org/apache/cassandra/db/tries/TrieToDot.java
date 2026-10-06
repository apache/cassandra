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
public class TrieToDot<T, D extends RangeState<D>> extends TriePathReconstructor implements DeletionAwareCursor.DeletionAwareWalker<T, D, String>
{
    private final StringBuilder b;
    private final Function<T, String> contentToString;
    private final Function<D, String> deletionBoundaryToString;
    private final Function<Integer, String> transitionToString;
    private final boolean useMultiByte;
    private int prevPos;
    private int currNodeTextPos;
    private int depthAdjustment;
    private boolean inDeletionBranch;

    public TrieToDot(Function<T, String> contentToString,
                     Function<Integer, String> transitionToString,
                     boolean useMultiByte)
    {
        this(contentToString, null, transitionToString, useMultiByte);
    }

    public TrieToDot(Function<T, String> contentToString,
                     Function<D, String> deletionBoundaryToString,
                     Function<Integer, String> transitionToString,
                     boolean useMultiByte)
    {
        this.contentToString = contentToString;
        this.deletionBoundaryToString = deletionBoundaryToString;
        this.transitionToString = transitionToString;
        this.useMultiByte = useMultiByte;
        this.b = new StringBuilder();
        b.append("digraph G {\n" +
                 "  splines=curved");
        addNodeDefinition(nodeString(0));
        depthAdjustment = 0;
        inDeletionBranch = false;
    }

    @Override
    public void resetPathLength(int newLength)
    {
        newLength += depthAdjustment;
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
        b.append(" -> ");
        String newNode = nodeString(keyPos);
        b.append(newNode);
        b.append(" [label=\"");
        for (int i = prevPos; i < keyPos - 1; ++i)
            b.append(transitionToString.apply(keyBytes[i] & 0xFF));
        b.append(transitionToString.apply(nextByte));
        b.append("\"]");
        addNodeDefinition(newNode);
    }

    private void addNodeDefinition(String newNode)
    {
        prevPos = keyPos;
        newLineAndIndent();
        currNodeTextPos = b.length();
        b.append(String.format("%s [shape=circle label=\"\"]", newNode));
    }

    private String nodeString(int keyPos)
    {
        StringBuilder r = new StringBuilder();
        r.append(inDeletionBranch ? "NodeD_" : "Node_");
        for (int i = 0; i < keyPos; ++i)
            r.append(transitionToString.apply(keyBytes[i] & 0xFF));
        return r.toString();
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
        b.replace(currNodeTextPos, b.length(), String.format("%s [shape=doublecircle label=\"%s\"]", nodeString(keyPos), contentToString.apply(content)));
    }

    @Override
    public String complete()
    {
        b.append("\n}\n");
        return b.toString();
    }

    @Override
    public boolean enterDeletionsBranch()
    {
        newLineAndIndent();
        String oldNode = nodeString(keyPos);
        b.append(oldNode);
        inDeletionBranch = true;
        String newNode = nodeString(keyPos);
        b.append(" -> ");
        addNodeDefinition(newNode);

        newLineAndIndent();
        b.append("{ rank=same; ").append(oldNode).append("; ").append(newNode).append("; }");

        depthAdjustment = keyPos;
        return true;
    }

    @Override
    public void deletionMarker(D marker)
    {
        b.replace(currNodeTextPos, b.length(), String.format("%s [shape=doublecircle label=\"%s\"]", nodeString(keyPos), deletionBoundaryToString.apply(marker)));
    }

    @Override
    public void exitDeletionsBranch()
    {
        resetPathLength(0);
        depthAdjustment = 0;
        inDeletionBranch = false;
    }
}
