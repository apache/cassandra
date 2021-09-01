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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.TokenSerializer;
import org.apache.cassandra.gms.VersionedValue;

import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

public class Utils
{
    static Token currentToken()
    {
        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(getBroadcastAddressAndPort());
        VersionedValue value = epState.getApplicationState(ApplicationState.TOKENS);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(value.toBytes())))
        {
            return TokenSerializer.deserialize(getPartitioner(), in).iterator().next();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    static List<Token> parseTokens(Collection<String> tokens)
    {
        return tokens.stream()
                .map(Utils::parseToken)
                .collect(Collectors.toList());
    }

    static List<Range<Token>> parseTokenRanges(Collection<Map.Entry<String, String>> tokenRanges)
    {
        return tokenRanges.stream()
                .map(Utils::parseTokenRange)
                .collect(Collectors.toList());
    }

    static Token parseToken(String token)
    {
        return getPartitioner().getTokenFactory().fromString(token);
    }

    static Range<Token> parseTokenRange(Map.Entry<String, String> tokenRange)
    {
        return parseTokenRange(tokenRange.getKey(), tokenRange.getValue());
    }

    static Range<Token> parseTokenRange(String exclusiveLowerBound, String inclusiveUpperBound)
    {
        return new Range<>(parseToken(exclusiveLowerBound), parseToken(inclusiveUpperBound));
    }
}
