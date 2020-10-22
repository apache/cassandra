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

package org.apache.cassandra.hadoop.cql3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;

public class CqlClientHelper
{
    private CqlClientHelper()
    {
    }

    public static Map<TokenRange, List<Host>> getLocalPrimaryRangeForDC(String keyspace, Metadata metadata, String targetDC)
    {
        Objects.requireNonNull(keyspace, "keyspace");
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(targetDC, "targetDC");

        // In 2.1 the logic was to have a set of nodes used as a seed, they were used to query
        // client.describe_local_ring(keyspace) -> List<TokenRange>; this should include all nodes in the local dc.
        // TokenRange contained the endpoints in order, so .endpoints.get(0) is the primary owner
        // Client does not have a similar API, instead it returns Set<Host>.  To replicate this we first need
        // to compute the primary owners, then add in the replicas

        List<Token> tokens = new ArrayList<>();
        Map<Token, Host> tokenToHost = new HashMap<>();
        for (Host host : metadata.getAllHosts())
        {
            if (!targetDC.equals(host.getDatacenter()))
                continue;

            for (Token token : host.getTokens())
            {
                Host previous = tokenToHost.putIfAbsent(token, host);
                if (previous != null)
                    throw new IllegalStateException("Two hosts share the same token; hosts " + host.getHostId() + ":"
                                                    + host.getTokens() + ", " + previous.getHostId() + ":" + previous.getTokens());
                tokens.add(token);
            }
        }
        Collections.sort(tokens);

        Map<TokenRange, List<Host>> rangeToReplicas = new HashMap<>();

        // The first token in the ring uses the last token as its 'start', handle this here to simplify the loop
        Token start = tokens.get(tokens.size() - 1);
        Token end = tokens.get(0);

        addRange(keyspace, metadata, tokenToHost, rangeToReplicas, start, end);
        for (int i = 1; i < tokens.size(); i++)
        {
            start = tokens.get(i - 1);
            end = tokens.get(i);

            addRange(keyspace, metadata, tokenToHost, rangeToReplicas, start, end);
        }

        return rangeToReplicas;
    }

    private static void addRange(String keyspace,
                                 Metadata metadata,
                                 Map<Token, Host> tokenToHost,
                                 Map<TokenRange, List<Host>> rangeToReplicas,
                                 Token start, Token end)
    {
        Host host = tokenToHost.get(end);
        String dc = host.getDatacenter();

        TokenRange range = metadata.newTokenRange(start, end);
        List<Host> replicas = new ArrayList<>();
        replicas.add(host);
        // get all the replicas for the specific DC
        for (Host replica : metadata.getReplicas(keyspace, range))
        {
            if (dc.equals(replica.getDatacenter()) && !host.equals(replica))
                replicas.add(replica);
        }
        List<Host> previous = rangeToReplicas.put(range, replicas);
        if (previous != null)
            throw new IllegalStateException("Two hosts (" + host + ", " + previous + ") map to the same token range: " + range);
    }
}
