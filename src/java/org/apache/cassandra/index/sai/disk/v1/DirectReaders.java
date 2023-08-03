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

package org.apache.cassandra.index.sai.disk.v1;

import java.util.function.Supplier;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;

public class DirectReaders
{
    public static void checkBitsPerValue(int valuesBitsPerValue, IndexInput input, Supplier<String> source) throws CorruptIndexException
    {
        if (valuesBitsPerValue > 64)
        {
            String message = String.format("%s is corrupted: Bits per value for block offsets must be no more than 64 and is %d", source.get(), valuesBitsPerValue);
            throw new CorruptIndexException(message, input);
        }
    }
}
