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
package org.apache.cassandra.dht;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;

public interface IPartitioner
{
    /**
     * Transform key to object representation of the on-disk format.
     *
     * @param key the raw, client-facing key
     * @return decorated version of key
     */
    public DecoratedKey decorateKey(ByteBuffer key);

    /**
     * Calculate a Token representing the approximate "middle" of the given
     * range.
     *
     * @return The approximate midpoint between left and right.
     */
    public Token midpoint(Token left, Token right);

    /**
     * Calculate a Token which take approximate 0 <= ratioToLeft <= 1 ownership of the given range.
     */
    public Token split(Token left, Token right, double ratioToLeft);

    /**
     * @return A Token smaller than all others in the range that is being partitioned.
     * Not legal to assign to a node or key.  (But legal to use in range scans.)
     */
    public Token getMinimumToken();

    /**
     * The biggest token for this partitioner, unlike getMinimumToken, this token is actually used and users wanting to
     * include all tokens need to do getMaximumToken().maxKeyBound()
     *
     * Not implemented for the ordered partitioners
     */
    default Token getMaximumToken()
    {
        throw new UnsupportedOperationException("If you are using a splitting partitioner, getMaximumToken has to be implemented");
    }

    /**
     * @return a Token that can be used to route a given key
     * (This is NOT a method to create a Token from its string representation;
     * for that, use TokenFactory.fromString.)
     */
    public Token getToken(ByteBuffer key);

    /**
     * @return a randomly generated token
     */
    public Token getRandomToken();

    /**
     * @param random instance of Random to use when generating the token
     *
     * @return a randomly generated token
     */
    public Token getRandomToken(Random random);

    public Token.TokenFactory getTokenFactory();

    /**
     * @return True if the implementing class preserves key order in the Tokens
     * it generates.
     */
    public boolean preservesOrder();

    /**
     * Calculate the deltas between tokens in the ring in order to compare
     *  relative sizes.
     *
     * @param sortedTokens a sorted List of Tokens
     * @return the mapping from 'token' to 'percentage of the ring owned by that token'.
     */
    public Map<Token, Float> describeOwnership(List<Token> sortedTokens);

    public AbstractType<?> getTokenValidator();

    /**
     * Abstract type that orders the same way as DecoratedKeys provided by this partitioner.
     * Used by secondary indices.
     */
    public AbstractType<?> partitionOrdering();

    default Optional<Splitter> splitter()
    {
        return Optional.empty();
    }
}
