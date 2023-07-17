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

package org.apache.cassandra.index.sai.disk.v1.sortedterms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

/**
 * Metadata produced by {@link SortedTermsWriter}, needed by {@link SortedTermsReader}.
 */
public class SortedTermsMeta
{
    public final long termCount;
    public final int maxTermLength;
    public List<SortedTermsSegmentMeta> segments;

    public SortedTermsMeta(DataInput input) throws IOException
    {
        termCount = input.readLong();
        maxTermLength = input.readInt();
        int numberOfSegments = input.readInt();
        segments = new ArrayList<>(numberOfSegments);
        for (int index = 0; index < numberOfSegments; index++)
            segments.add(new SortedTermsSegmentMeta(input));
    }

    public static void write(IndexOutput output, long termCount, int maxTermLength, List<SortedTermsSegmentMeta> segments) throws IOException
    {
        output.writeLong(termCount);
        output.writeInt(maxTermLength);
        output.writeInt(segments.size());
        for (SortedTermsSegmentMeta segment : segments)
            segment.write(output);
    }

    public static class SortedTermsSegmentMeta
    {
        public final long trieFilePointer;
        public final BytesRef minimumTerm;
        public final BytesRef maximumTerm;

        public SortedTermsSegmentMeta(DataInput input) throws IOException
        {
            trieFilePointer = input.readLong();
            minimumTerm = readBytes(input);
            maximumTerm = readBytes(input);
        }

        public SortedTermsSegmentMeta(long trieFilePointer, BytesRef minimumTerm, BytesRef maximumTerm)
        {
            this.trieFilePointer = trieFilePointer;
            this.minimumTerm = minimumTerm;
            this.maximumTerm = maximumTerm;
        }

        void write(IndexOutput indexOutput) throws IOException
        {
            indexOutput.writeLong(trieFilePointer);
            writeBytes(minimumTerm, indexOutput);
            writeBytes(maximumTerm, indexOutput);
        }

        private static BytesRef readBytes(DataInput input) throws IOException
        {
            int len = input.readInt();
            byte[] bytes = new byte[len];
            input.readBytes(bytes, 0, len);
            return new BytesRef(bytes);
        }

        private static void writeBytes(BytesRef buffer, IndexOutput out)
        {
            try
            {
                out.writeInt(buffer.length);
                out.writeBytes(buffer.bytes, 0, buffer.length);
            }
            catch (IOException ioe)
            {
                throw new RuntimeException(ioe);
            }
        }
    }
}
