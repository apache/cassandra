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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.StringType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public abstract class AbstractAnalyzer implements Iterator<ByteBuffer>
{
    protected ByteBuffer next = null;
    protected String nextLiteral = null;

    /**
     * @return true if index value is transformed, e.g. normalized or lower-cased or tokenized.
     */
    public abstract boolean transformValue();

    /**
     * Call when tokenization is finished.  Used by the LuceneAnalyzer.
     */
    public void end()
    {
    }

    /**
     * Note: This method does not advance, as we rely on {@link #hasNext()} to buffer the next value.
     *
     * @return the raw value currently buffered by this iterator
     */
    @Override
    public ByteBuffer next()
    {
        if (next == null)
            throw new NoSuchElementException();
        return next;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    protected abstract void resetInternal(ByteBuffer input);

    public void reset(ByteBuffer input)
    {
        this.next = null;
        this.nextLiteral = null;

        resetInternal(input);
    }

    public interface AnalyzerFactory
    {
        AbstractAnalyzer create();

        default void close()
        {
        }
    }

    public static AnalyzerFactory fromOptions(AbstractType<?> type, Map<String, String> options)
    {
        if (hasNonTokenizingOptions(options))
        {
            if (type instanceof StringType)
            {
                // validate options
                NonTokenizingOptions.fromMap(options);
                return () -> new NonTokenizingAnalyzer(type, options);
            }
            else
            {
                throw new InvalidRequestException("CQL type " + type.asCQL3Type() + " cannot be analyzed.");
            }
        }

        return NoOpAnalyzer::new;
    }

    private static boolean hasNonTokenizingOptions(Map<String, String> options)
    {
        return options.get(NonTokenizingOptions.ASCII) != null ||
               options.containsKey(NonTokenizingOptions.CASE_SENSITIVE) ||
               options.containsKey(NonTokenizingOptions.NORMALIZE);
    }

}
