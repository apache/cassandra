/**
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
import java.nio.charset.CharacterCodingException;
import java.text.Collator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;

public class CollatingOrderPreservingPartitioner extends AbstractByteOrderedPartitioner
{
    static final Collator collator = Collator.getInstance(new Locale("en", "US"));

    public BytesToken getToken(ByteBuffer key)
    {
        if (key.remaining() == 0)
            return MINIMUM;

        String skey;
        try
        {
            skey = ByteBufferUtil.string(key);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException("The provided key was not UTF8 encoded.", e);
        }
        return new BytesToken(ByteBuffer.wrap(collator.getCollationKey(skey).toByteArray()));
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens){ throw new UnsupportedOperationException(); }
}
