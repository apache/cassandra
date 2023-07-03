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
package org.apache.cassandra.index.sai.disk.v1.bbtree;

import java.io.IOException;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.TermsIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * {@link IntersectingPointValues} that prevents buffered points from reordering, and always skips the sorting phase in Lucene
 * It's the responsibility of the underlying implementation to ensure that all points are correctly sorted.
 */
public class ImmutableIntersectingPointValues extends IntersectingPointValues
{
    private final TermsIterator termEnum;
    private final byte[] scratch;

    private ImmutableIntersectingPointValues(TermsIterator termEnum, AbstractType<?> termComparator)
    {
        this.termEnum = termEnum;
        this.scratch = new byte[TypeUtil.fixedSizeOf(termComparator)];
    }

    public static ImmutableIntersectingPointValues fromTermEnum(TermsIterator termEnum, AbstractType<?> termComparator)
    {
        return new ImmutableIntersectingPointValues(termEnum, termComparator);
    }

    @Override
    public void intersect(IntersectVisitor visitor) throws IOException
    {
        while (termEnum.hasNext())
        {
            ByteSourceInverse.copyBytes(termEnum.next().asComparableBytes(ByteComparable.Version.OSS50), scratch);
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
