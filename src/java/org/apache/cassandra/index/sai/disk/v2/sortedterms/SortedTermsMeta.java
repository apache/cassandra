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

package org.apache.cassandra.index.sai.disk.v2.sortedterms;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Metadata produced by {@link SortedTermsWriter}, needed by {@link SortedTermsReader}.
 */
public class SortedTermsMeta
{
    public final long trieFP;
    /** Number of terms */
    public final long count;
    public final int maxTermLength;

    public SortedTermsMeta(IndexInput input) throws IOException
    {
        this.trieFP = input.readLong();
        this.count = input.readLong();
        this.maxTermLength = input.readInt();
    }

    public SortedTermsMeta(long trieFP, long count, int maxTermLength)
    {
        this.trieFP = trieFP;
        this.count = count;
        this.maxTermLength = maxTermLength;
    }

    public void write(IndexOutput output) throws IOException
    {
        output.writeLong(trieFP);
        output.writeLong(count);
        output.writeInt(maxTermLength);
    }
}
