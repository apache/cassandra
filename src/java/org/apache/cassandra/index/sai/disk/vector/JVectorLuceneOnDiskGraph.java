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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;

import io.github.jbellis.jvector.util.Bits;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.v1.postings.VectorPostingList;

/**
 * A common interface between Lucene and JVector graph indexes
 */
public interface JVectorLuceneOnDiskGraph extends AutoCloseable
{
    long ramBytesUsed();

    int size();

    OnDiskOrdinalsMap.OrdinalsView getOrdinalsView() throws IOException;

    /**
     * See CassandraDiskANN::search
     */
    VectorPostingList search(float[] queryVector, int topK, int limit, Bits bits, QueryContext context);

    void close() throws IOException;
}
