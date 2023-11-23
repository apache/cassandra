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

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Utility class for creating and reading the column completion marker, {@link IndexComponent#COLUMN_COMPLETION_MARKER}.
 * </p>
 * The file has a header and a footer, as written by {@link SAICodecUtils#writeHeader(IndexOutput)} and
 * {@link SAICodecUtils#writeFooter(IndexOutput)}. The only content of the file is a single byte indicating whether the
 * column index is empty or not. If the index is empty the completion marker will be the only per-index component.
 */
public class ColumnCompletionMarkerUtil
{
    private static final byte EMPTY = (byte) 1;
    private static final byte NOT_EMPTY = (byte) 0;

    /**
     * Creates a column index completion marker for the specified column index, storing in it whether the index is empty.
     *
     * @param descriptor the index descriptor
     * @param indexIdentifier the column index identifier
     * @param isEmpty whether the index is empty
     */
    public static void create(IndexDescriptor descriptor, IndexIdentifier indexIdentifier, boolean isEmpty) throws IOException
    {
        try (IndexOutputWriter output = descriptor.openPerIndexOutput(IndexComponent.COLUMN_COMPLETION_MARKER, indexIdentifier))
        {
            SAICodecUtils.writeHeader(output);
            output.writeByte(isEmpty ? EMPTY : NOT_EMPTY);
            SAICodecUtils.writeFooter(output);
        }
    }

    /**
     * Reads the column index completion marker and returns whether if the index is empty.
     *
     * @param descriptor the index descriptor
     * @param indexIdentifier the column index identifier
     * @return {@code true} if the index is empty, {@code false} otherwise.
     */
    public static boolean isEmptyIndex(IndexDescriptor descriptor, IndexIdentifier indexIdentifier) throws IOException
    {
        try (IndexInput input = descriptor.openPerIndexInput(IndexComponent.COLUMN_COMPLETION_MARKER, indexIdentifier))
        {
            SAICodecUtils.checkHeader(input); // consume header
            return input.readByte() == EMPTY;
        }
    }
}
