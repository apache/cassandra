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
package org.apache.cassandra.hadoop.cql3;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.utils.Pair;

/**
 * Implements an iterable-friendly {@link CqlPagingRecordReader}.
 */
public class IterableCqlPagingRecordReader extends CqlPagingRecordReader
  implements Iterable<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>>, Closeable
{
    public Iterator<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> iterator()
    {
        return new Iterator<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>>()
        {
            public boolean hasNext()
            {
                return rowIterator.hasNext();
            }

            public Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> next()
            {
                return rowIterator.next();
            }

            public void remove()
            {
                throw new UnsupportedOperationException("Cannot remove an element on this iterator!");

            }
        };
    }

    /**
     * @throws NotImplementedException Always throws this exception, this operation does not make sense in this implementation.
     */
    @Override
    public boolean nextKeyValue() throws IOException
    {
        throw new UnsupportedOperationException("Calling method nextKeyValue() does not make sense in this implementation");
    }

    /**
     * @throws NotImplementedException Always throws this exception, this operation does not make sense in this implementation.
     */
    @Override
    public boolean next(Map<String, ByteBuffer> keys, Map<String, ByteBuffer> value) throws IOException
    {
        throw new UnsupportedOperationException("Calling method next() does not make sense in this implementation");
    }
}
