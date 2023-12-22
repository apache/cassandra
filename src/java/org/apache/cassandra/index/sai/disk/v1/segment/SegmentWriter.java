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

package org.apache.cassandra.index.sai.disk.v1.segment;

import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.index.sai.utils.IndexEntry;

public interface SegmentWriter
{
    /**
     * Appends a set of terms and associated postings to their respective overall SSTable component files.
     *
     * @param indexEntryIterator an {@link Iterator} of {@link IndexEntry}s sorted in term order.
     *
     * @return metadata describing the location of this inverted index in the overall SSTable terms and postings component files
     */
    SegmentMetadata.ComponentMetadataMap writeCompleteSegment(Iterator<IndexEntry> indexEntryIterator) throws IOException;

    /**
     * Returns the number of rows written to the segment
     *
     * @return the number of rows
     */
    long getNumberOfRows();
}
