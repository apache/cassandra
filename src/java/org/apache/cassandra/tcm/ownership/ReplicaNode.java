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

package org.apache.cassandra.tcm.ownership;

import java.util.Objects;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tcm.membership.NodeId;

public class ReplicaNode
{
    public final NodeId nodeId;
    public final Range<Token> range;
    public final boolean full;

    public ReplicaNode(NodeId nodeId, Range<Token> range, boolean full)
    {
        this.nodeId = nodeId;
        this.range = range;
        this.full = full;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ReplicaNode)) return false;
        ReplicaNode that = (ReplicaNode) o;
        return full == that.full && Objects.equals(nodeId, that.nodeId) && Objects.equals(range, that.range);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeId, range, full);
    }

    @Override
    public String toString()
    {
        return "ReplicaNode{" +
               "nodeId=" + nodeId +
               ", range=" + range +
               ", full=" + full +
               '}';
    }
}
