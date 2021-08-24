/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

package org.apache.cassandra.index.sai.analyzer;

import java.io.CharArrayReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.disk.io.BytesRefUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;

public class LuceneAnalyzer extends AbstractAnalyzer
{
    public static final String INDEX_ANALYZER = "index_analyzer";
    public static final String QUERY_ANALYZER = "query_analyzer";
    private AbstractType<?> type;
    private boolean hasNext = false;

    private final Analyzer analyzer;
    private TokenStream tokenStream;
    private final CharsRefBuilder charsBuilder = new CharsRefBuilder();
    private TermToBytesRefAttribute termAttr;
    private final BytesRefBuilder bytesBuilder = new BytesRefBuilder();
    private final Map<String, String> options;

    public LuceneAnalyzer(AbstractType<?> type, Analyzer analyzer, Map<String, String> options)
    {
        this.type = type;
        this.analyzer = analyzer;
        this.options = options;
    }

    @Override
    public boolean hasNext()
    {
        if (tokenStream == null)
        {
            throw new IllegalStateException("resetInternal(ByteBuffer term) must be called prior to hasNext()");
        }
        try
        {
            hasNext = tokenStream.incrementToken();

            if (hasNext)
            {
                final BytesRef br = termAttr.getBytesRef();
                // TODO: should be able to reuse the bytes ref however
                //       MemoryIndex#setMinMaxTerm requires a copy
                //       getting the max term from the mem trie is best
                next = ByteBuffer.wrap(BytesRef.deepCopyOf(br).bytes);
            }
            return hasNext;
        }
        catch (IOException ex)
        {
            throw Throwables.cleaned(ex);
        }
    }

    @Override
    public ByteBuffer next()
    {
        if (!hasNext)
        {
            throw new NoSuchElementException();
        }
        return next;
    }

    @Override
    public void end()
    {
        try
        {
            try
            {
                tokenStream.end();
            }
            finally
            {
                tokenStream.close();
            }
        }
        catch (IOException ex)
        {
            throw Throwables.cleaned(ex); // highly unlikely exception
        }
    }

    @Override
    public boolean transformValue()
    {
        return true;
    }

    @Override
    protected void resetInternal(ByteBuffer input)
    {
        try
        {
            // the following uses a byte[] and char[] buffer to reduce object creation
            BytesRefUtil.copyBufferToBytesRef(input, bytesBuilder);

            charsBuilder.copyUTF8Bytes(bytesBuilder.get());

            final CharsRef charsRef = charsBuilder.get();

            // the field name doesn't matter here, it's an internal lucene thing
            tokenStream = analyzer.tokenStream("field", new CharArrayReader(charsRef.chars, charsRef.offset, charsRef.length));
            tokenStream.reset();
            termAttr = tokenStream.getAttribute(TermToBytesRefAttribute.class);

            this.hasNext = true;
        }
        catch (Exception ex)
        {
            throw Throwables.cleaned(ex);
        }
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("type", type)
                          .add("hasNext", hasNext)
                          .add("analyzer", analyzer)
                          .add("tokenStream", tokenStream)
                          .add("termAttr", termAttr)
                          .add("options", options)
                          .toString();
    }
}
