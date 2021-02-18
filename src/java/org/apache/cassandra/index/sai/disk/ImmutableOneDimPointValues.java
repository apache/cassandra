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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.util.bkd.BKDWriter;

/**
 * {@link MutablePointValues} that prevents buffered points from reordering, and always skips sorting phase in Lucene
 * It's the responsibility of the underlying implementation to ensure that all points are correctly sorted.
 * <p>
 * It allows to take advantage of an optimised 1-dim writer {@link BKDWriter}
 * (that is enabled only for {@link MutablePointValues}), and reduce number of times we sort point values.
 */
public class ImmutableOneDimPointValues extends MutableOneDimPointValues
{
    private final TermsIterator termEnum;
    private final AbstractType termComparator;
    private final byte[] scratch;

    private ImmutableOneDimPointValues(TermsIterator termEnum, AbstractType<?> termComparator)
    {
        this.termEnum = termEnum;
        this.termComparator = termComparator;
        this.scratch = new byte[TypeUtil.fixedSizeOf(termComparator)];
    }

    public static ImmutableOneDimPointValues fromTermEnum(TermsIterator termEnum, AbstractType<?> termComparator)
    {
        return new ImmutableOneDimPointValues(termEnum, termComparator);
    }

    @Override
    public void intersect(IntersectVisitor visitor) throws IOException
    {
        while (termEnum.hasNext())
        {
            ByteBufferUtil.toBytes(termEnum.next().asComparableBytes(ByteComparable.Version.OSS41), scratch);
            try (final PostingList postings = termEnum.postings())
            {
                long segmentRowId;
                while ((segmentRowId = postings.nextPosting()) != PostingList.END_OF_STREAM)
                {
                    visitor.visit(segmentRowId, scratch);
                }
            }
        }
    }

    @Override
    public int getBytesPerDimension()
    {
        return scratch.length;
    }
}
