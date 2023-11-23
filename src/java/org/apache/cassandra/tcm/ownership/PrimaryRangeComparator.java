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

import java.util.Comparator;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;

public class PrimaryRangeComparator implements Comparator<Replica>
{
    private final TokenMap tokens;
    private final Directory directory;

    public PrimaryRangeComparator(TokenMap tokens, Directory directory)
    {
        this.tokens = tokens;
        this.directory = directory;
    }

    @Override
    public int compare(Replica o1, Replica o2)
    {
        assert o1.range().equals(o2.range());
        Token target = o1.range().right.equals(tokens.partitioner().getMinimumToken())
                       ? tokens.tokens().get(0)
                       : o1.range().right;
        NodeId owner = tokens.owner(target);
        return directory.peerId(o1.endpoint()).equals(owner)
               ? -1
               : directory.peerId(o2.endpoint()).equals(owner) ? 1 : 0;
    }
}
