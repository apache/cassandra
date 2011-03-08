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

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

public class CollatingOrderPreservingPartitionerTest extends PartitionerTestCase<BytesToken>
{
    public void initPartitioner()
    {
        partitioner = new CollatingOrderPreservingPartitioner();
    }

    /**
     * Test that a non-UTF-8 byte array can still be encoded.
     */
    @Test
    public void testTokenFactoryStringsNonUTF()
    {
        Token.TokenFactory factory = this.partitioner.getTokenFactory();
        BytesToken tok = new BytesToken(new byte[]{(byte)0xFF, (byte)0xFF});
        assert tok.compareTo(factory.fromString(factory.toString(tok))) == 0;
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
}
