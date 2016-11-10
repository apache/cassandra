/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.dht;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.config.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.Util.dk;


/**
 * Test cases where multiple keys collides, ie have the same token.
 * Order preserving partitioner have no possible collision and creating
 * collision for the RandomPartitioner is ... difficult, so we create a dumb
 * length partitioner that takes the length of the key as token, making
 * collision easy and predictable.
 */
public class KeyCollisionTest
{
    static IPartitioner oldPartitioner;
    private static final String KEYSPACE1 = "KeyCollisionTest1";
    private static final String CF = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        oldPartitioner = DatabaseDescriptor.getPartitioner();
        DatabaseDescriptor.setPartitioner(LengthPartitioner.instance);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF));
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setPartitioner(oldPartitioner);
    }

    @Test
    public void testGetSliceWithCollision() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.clearUnsafe();

        insert("k1", "k2", "kq");       // token = 2, kq ordered after row below lexicographically
        insert("key1", "key2", "key3"); // token = 4
        insert("longKey1", "longKey2"); // token = 8

        List<Row> rows = cfs.getRangeSlice(new Bounds<RowPosition>(dk("k2"), dk("key2")), null, new IdentityQueryFilter(), 10000);
        assert rows.size() == 4 : "Expecting 4 keys, got " + rows.size();
        assert rows.get(0).key.getKey().equals(ByteBufferUtil.bytes("k2"));
        assert rows.get(1).key.getKey().equals(ByteBufferUtil.bytes("kq"));
        assert rows.get(2).key.getKey().equals(ByteBufferUtil.bytes("key1"));
        assert rows.get(3).key.getKey().equals(ByteBufferUtil.bytes("key2"));
    }

    private void insert(String... keys)
    {
        for (String key : keys)
            insert(key);
    }

    private void insert(String key)
    {
        Mutation rm;
        rm = new Mutation(KEYSPACE1, ByteBufferUtil.bytes(key));
        rm.add(CF, Util.cellname("column"), ByteBufferUtil.bytes("asdf"), 0);
        rm.applyUnsafe();
    }

    static class BigIntegerToken extends ComparableObjectToken<BigInteger>
    {
        private static final long serialVersionUID = 1L;

        public BigIntegerToken(BigInteger token)
        {
            super(token);
        }

        // convenience method for testing
        public BigIntegerToken(String token) {
            this(new BigInteger(token));
        }

        @Override
        public IPartitioner getPartitioner()
        {
            return LengthPartitioner.instance;
        }

        @Override
        public long getHeapSize()
        {
            return 0;
        }
    }

    public static class LengthPartitioner implements IPartitioner
    {
        public static final BigInteger ZERO = new BigInteger("0");
        public static final BigIntegerToken MINIMUM = new BigIntegerToken("-1");

        public static LengthPartitioner instance = new LengthPartitioner();

        public DecoratedKey decorateKey(ByteBuffer key)
        {
            return new BufferDecoratedKey(getToken(key), key);
        }

        public BigIntegerToken midpoint(Token ltoken, Token rtoken)
        {
            // the symbolic MINIMUM token should act as ZERO: the empty bit array
            BigInteger left = ltoken.equals(MINIMUM) ? ZERO : ((BigIntegerToken)ltoken).token;
            BigInteger right = rtoken.equals(MINIMUM) ? ZERO : ((BigIntegerToken)rtoken).token;
            Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(left, right, 127);
            // discard the remainder
            return new BigIntegerToken(midpair.left);
        }

        public BigIntegerToken getMinimumToken()
        {
            return MINIMUM;
        }

        public BigIntegerToken getRandomToken()
        {
            return new BigIntegerToken(BigInteger.valueOf(new Random().nextInt(15)));
        }

        private final Token.TokenFactory tokenFactory = new Token.TokenFactory() {
            public ByteBuffer toByteArray(Token token)
            {
                BigIntegerToken bigIntegerToken = (BigIntegerToken) token;
                return ByteBuffer.wrap(bigIntegerToken.token.toByteArray());
            }

            public Token fromByteArray(ByteBuffer bytes)
            {
                return new BigIntegerToken(new BigInteger(ByteBufferUtil.getArray(bytes)));
            }

            public String toString(Token token)
            {
                BigIntegerToken bigIntegerToken = (BigIntegerToken) token;
                return bigIntegerToken.token.toString();
            }

            public Token fromString(String string)
            {
                return new BigIntegerToken(new BigInteger(string));
            }

            public void validate(String token) {}
        };

        public Token.TokenFactory getTokenFactory()
        {
            return tokenFactory;
        }

        public boolean preservesOrder()
        {
            return false;
        }

        public BigIntegerToken getToken(ByteBuffer key)
        {
            if (key.remaining() == 0)
                return MINIMUM;
            return new BigIntegerToken(BigInteger.valueOf(key.remaining()));
        }

        public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
        {
            // allTokens will contain the count and be returned, sorted_ranges is shorthand for token<->token math.
            Map<Token, Float> allTokens = new HashMap<Token, Float>();
            List<Range<Token>> sortedRanges = new ArrayList<Range<Token>>();

            // this initializes the counts to 0 and calcs the ranges in order.
            Token lastToken = sortedTokens.get(sortedTokens.size() - 1);
            for (Token node : sortedTokens)
            {
                allTokens.put(node, new Float(0.0));
                sortedRanges.add(new Range<Token>(lastToken, node));
                lastToken = node;
            }

            for (String ks : Schema.instance.getKeyspaces())
            {
                for (CFMetaData cfmd : Schema.instance.getKSMetaData(ks).cfMetaData().values())
                {
                    for (Range<Token> r : sortedRanges)
                    {
                        // Looping over every KS:CF:Range, get the splits size and add it to the count
                        allTokens.put(r.right, allTokens.get(r.right) + StorageService.instance.getSplits(ks, cfmd.cfName, r, 1).size());
                    }
                }
            }

            // Sum every count up and divide count/total for the fractional ownership.
            Float total = new Float(0.0);
            for (Float f : allTokens.values())
                total += f;
            for (Map.Entry<Token, Float> row : allTokens.entrySet())
                allTokens.put(row.getKey(), row.getValue() / total);

            return allTokens;
        }

        public AbstractType<?> getTokenValidator()
        {
            return IntegerType.instance;
        }
    }
}
