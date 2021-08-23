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

import java.nio.ByteBuffer;
import java.text.Normalizer;
import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;

public abstract class AbstractAnalyzer implements Iterator<ByteBuffer>
{
    protected ByteBuffer next = null;

    public ByteBuffer next()
    {
        return next;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void validate(Map<String, String> options, ColumnMetadata cm) throws ConfigurationException
    {
        if (!isCompatibleWith(cm.type))
            throw new ConfigurationException(String.format("%s does not support type %s",
                                                           this.getClass().getSimpleName(),
                                                           cm.type.asCQL3Type()));
    }

    public abstract void init(Map<String, String> options, AbstractType<?> validator);

    public abstract void reset(ByteBuffer input);

    /**
     * Test whether the given validator is compatible with the underlying analyzer.
     *
     * @param validator the validator to test the compatibility with
     * @return true if the give validator is compatible, false otherwise
     */
    protected abstract boolean isCompatibleWith(AbstractType<?> validator);

    /**
     * @return true if current analyzer provides text tokenization, false otherwise.
     */
    public boolean isTokenizing()
    {
        return false;
    }

    public static String normalize(String original)
    {
        return Normalizer.isNormalized(original, Normalizer.Form.NFC)
                ? original
                : Normalizer.normalize(original, Normalizer.Form.NFC);
    }
}
