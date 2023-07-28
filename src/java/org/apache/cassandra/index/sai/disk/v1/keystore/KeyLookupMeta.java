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

package org.apache.cassandra.index.sai.disk.v1.keystore;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Metadata produced by {@link KeyStoreWriter}, needed by {@link KeyLookup}.
 */
public class KeyLookupMeta
{
    public final long keyCount;
    public final int maxKeyLength;

    public KeyLookupMeta(DataInput input) throws IOException
    {
        this.keyCount = input.readLong();
        this.maxKeyLength = input.readInt();
    }

    public static void write(IndexOutput output, long keyCount, int maxKeyLength) throws IOException
    {
        output.writeLong(keyCount);
        output.writeInt(maxKeyLength);
    }
}
