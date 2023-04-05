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
package org.apache.cassandra.index.sasi.analyzer;

import java.nio.CharBuffer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.AbstractIterator;

@Beta
public class DelimiterAnalyzer extends AbstractAnalyzer
{

    private static final Map<AbstractType<?>, Charset> VALID_ANALYZABLE_TYPES = new HashMap<AbstractType<?>, Charset>()
    {{
        put(UTF8Type.instance, StandardCharsets.UTF_8);
        put(AsciiType.instance, StandardCharsets.US_ASCII);
    }};

    private char delimiter;
    private Charset charset;
    private Iterator<ByteBuffer> iter;

    public DelimiterAnalyzer()
    {
    }

    @Override
    public ByteBuffer next()
    {
        return iter.next();
    }

    public void init(Map<String, String> options, AbstractType<?> validator)
    {
        DelimiterTokenizingOptions tokenizingOptions = DelimiterTokenizingOptions.buildFromMap(options);
        delimiter = tokenizingOptions.getDelimiter();
        charset = VALID_ANALYZABLE_TYPES.get(validator);
    }

    public boolean hasNext()
    {
        return iter.hasNext();
    }

    public void reset(ByteBuffer input)
    {
        Preconditions.checkNotNull(input);
        final CharBuffer cb = charset.decode(input);

        this.iter = new AbstractIterator<ByteBuffer>() {
            protected ByteBuffer computeNext() {

                if (!cb.hasRemaining())
                    return endOfData();

                CharBuffer readahead = cb.duplicate();
                // loop until we see the next delimiter character, or reach end of data
                boolean readaheadRemaining;
                while ((readaheadRemaining = readahead.hasRemaining()) && readahead.get() != delimiter);

                char[] chars = new char[readahead.position() - cb.position() - (readaheadRemaining ? 1 : 0)];
                cb.get(chars);
                Preconditions.checkState(!cb.hasRemaining() || cb.get() == delimiter);

                return 0 < chars.length
                        ? charset.encode(CharBuffer.wrap(chars))
                        // blank partition keys not permitted, ref ConcurrentRadixTree.putIfAbsent(..)
                        : computeNext();
            }
        };
    }

    @Override
    public boolean isTokenizing()
    {
        return true;
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> validator)
    {
        return VALID_ANALYZABLE_TYPES.containsKey(validator);
    }
}
