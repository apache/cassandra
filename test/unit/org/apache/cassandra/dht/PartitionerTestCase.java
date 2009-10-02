/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.dht;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;

public abstract class PartitionerTestCase<T extends Token> {
    protected IPartitioner<T> partitioner;

    public abstract IPartitioner<T> getPartitioner();
    public abstract T tok(String string);
    public abstract String tos(T token);

    @Before
    public void clean()
    {
        this.partitioner = this.getPartitioner();
    }

    @Test
    public void testCompare()
    {
        assert tok("").compareTo(tok("asdf")) < 0;
        assert tok("asdf").compareTo(tok("")) > 0;
        assert tok("").compareTo(tok("")) == 0;
        assert tok("z").compareTo(tok("a")) > 0;
        assert tok("a").compareTo(tok("z")) < 0;
        assert tok("asdf").compareTo(tok("asdf")) == 0;
        assert tok("asdz").compareTo(tok("asdf")) > 0;
    }

    public void assertMidpoint(T left, T right, int depth)
    {
        T mid = this.partitioner.midpoint(left, right);
        assert new Range(left, right).contains(mid)
                : "For " + tos(left) + "," + tos(right) + ": range did not contain mid:" + tos(mid);
        if (depth > 0)
            assertMidpoint(left, mid, depth-1);
        if (depth > 0)
            assertMidpoint(mid, right, depth-1);
    }

    @Test
    public void testMidpoint()
    {
        assertMidpoint(tok("a"), tok("b"), 16);
        assertMidpoint(tok("a"), tok("bbb"), 16);
    }

    @Test
    public void testMidpointMinimum()
    {
        assertMidpoint(tok(""), tok("a"), 16);
        assertMidpoint(tok(""), tok("aaa"), 16);
    }

    @Test
    public void testMidpointWrapping()
    {
        assertMidpoint(tok(""), tok(""), 16);
        assertMidpoint(tok("a"), tok(""), 16);
    }
    
    @Test
    public void testDiskFormat()
    {
        String key = "key";
        DecoratedKey<T> decKey = partitioner.decorateKey(key);
        DecoratedKey<T> result = partitioner.convertFromDiskFormat(partitioner.convertToDiskFormat(decKey));
        assertEquals(decKey, result);
    }
    
    @Test
    public void testTokenFactoryBytes()
    {
        Token.TokenFactory factory = this.partitioner.getTokenFactory();
        assert tok("a").compareTo(factory.fromByteArray(factory.toByteArray(tok("a")))) == 0;
    }
    
    @Test
    public void testTokenFactoryStrings()
    {
        Token.TokenFactory factory = this.partitioner.getTokenFactory();
        assert tok("a").compareTo(factory.fromString(factory.toString(tok("a")))) == 0;
    }
}
