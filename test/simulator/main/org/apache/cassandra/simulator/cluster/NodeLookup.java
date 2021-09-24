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

package org.apache.cassandra.simulator.cluster;

public class NodeLookup
{
    protected final int[] nodeToDc;
    protected final int[] nodeToToken;

    protected NodeLookup(int[] nodeToDc)
    {
        this.nodeToDc = nodeToDc;
        this.nodeToToken = new int[nodeToDc.length];
        for (int i = 0; i < nodeToToken.length; ++i)
            nodeToToken[i] = i + 1;
    }

    public int dcOf(int node)
    {
        return nodeToDc[node - 1];
    }

    public int tokenOf(int node)
    {
        return nodeToToken[node - 1];
    }

    public void setTokenOf(int node, int token)
    {
        nodeToToken[node - 1] = token;
    }
}
