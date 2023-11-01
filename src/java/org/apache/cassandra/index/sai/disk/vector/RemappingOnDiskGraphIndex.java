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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.util.function.IntUnaryOperator;

import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.NodesIterator;

/**
 * Remaps the node ordinals of a GraphIndex.
 */
public class RemappingOnDiskGraphIndex<T> implements GraphIndex<T>
{
    private final GraphIndex<T> graphIndex;
    private final IntUnaryOperator ordinalMapper;
    private final IntUnaryOperator reverseOrdinalMapper;

    private class RemappingNodesIterator extends NodesIterator
    {
        private final NodesIterator nodesIterator;

        public RemappingNodesIterator(int size, NodesIterator nodesIterator) {
            super(size);
            this.nodesIterator = nodesIterator;
        }

        @Override
        public boolean hasNext()
        {
            return nodesIterator.hasNext();
        }

        @Override
        public int nextInt()
        {
            return ordinalMapper.applyAsInt(nodesIterator.nextInt());
        }

        @Override
        public int size()
        {
            return nodesIterator.size();
        }
    }

    private class RemappingView<T> implements View<T>
    {
        private final View<T> view;
        public RemappingView(View<T> view) {
            this.view = view;
        }

        @Override
        public NodesIterator getNeighborsIterator(int i)
        {
            var it = view.getNeighborsIterator(reverseOrdinalMapper.applyAsInt(i));
            return new RemappingNodesIterator(it.size(), it);
        }

        @Override
        public int size()
        {
            return view.size();
        }

        @Override
        public int entryNode()
        {
            return ordinalMapper.applyAsInt(view.entryNode());
        }

        @Override
        public T getVector(int i)
        {
            return view.getVector(reverseOrdinalMapper.applyAsInt(i));
        }

        @Override
        public void close() throws Exception
        {
            view.close();
        }
    }

    public RemappingOnDiskGraphIndex(GraphIndex<T> graphIndex, IntUnaryOperator ordinalMapper,
                                     IntUnaryOperator reverseOrdinalMapper)
    {
        this.graphIndex = graphIndex;
        this.ordinalMapper = ordinalMapper;
        this.reverseOrdinalMapper = reverseOrdinalMapper;
    }

    @Override
    public int size()
    {
        return graphIndex.size();
    }

    @Override
    public NodesIterator getNodes()
    {
        return new RemappingNodesIterator(graphIndex.size(), graphIndex.getNodes());
    }

    @Override
    public View<T> getView()
    {
        return new RemappingView<>(graphIndex.getView());
    }

    @Override
    public int maxDegree()
    {
        return graphIndex.maxDegree();
    }

    @Override
    public void close() throws IOException
    {
        graphIndex.close();
    }
}