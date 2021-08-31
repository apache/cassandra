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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Iterator to step through terms to obtain {@link PostingList} for the current term.
 *
 * Term enumerations are always ordered by their {@link ByteSource}.
 */
@NotThreadSafe
public interface TermsIterator extends Iterator<ByteComparable>, Closeable
{
    /**
     * Get {@link PostingList} for the current term.
     */
    PostingList postings() throws IOException;

    ByteBuffer getMinTerm();

    ByteBuffer getMaxTerm();
}
