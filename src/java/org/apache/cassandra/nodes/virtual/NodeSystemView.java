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

package org.apache.cassandra.nodes.virtual;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.nodes.NodeInfo;
import org.apache.cassandra.schema.TableMetadata;

abstract class NodeSystemView extends AbstractVirtualTable
{
    NodeSystemView(TableMetadata metadata)
    {
        super(metadata);
    }

    DataSet completeRow(SimpleDataSet dataset, NodeInfo n)
    {
                      //+ "data_center text,"
        return dataset.column("data_center", n.getDataCenter())
                      //+ "host_id uuid,"
                      .column("host_id", n.getHostId())
                      //+ "rack text,"
                      .column("rack", n.getRack())
                      //+ "release_version text,"
                      .column("release_version", safeToString(n.getReleaseVersion()))
                      //+ "schema_version uuid,"
                      .column("schema_version", n.getSchemaVersion())
                      //+ "tokens set<varchar>,"
                      .column("tokens", tokensAsSet(n.getTokens()));
    }

    static String safeToString(Object o)
    {
        return o != null ? o.toString() : null;
    }

    private static Set<String> tokensAsSet(Collection<Token> tokens)
    {
        if (tokens == null)
            return null;
        if (tokens.isEmpty())
            return Collections.emptySet();
        Token.TokenFactory factory = DatabaseDescriptor.getPartitioner().getTokenFactory();
        Set<String> s = new HashSet<>(tokens.size());
        for (Token tk : tokens)
            s.add(factory.toString(tk));
        return s;
    }
}
