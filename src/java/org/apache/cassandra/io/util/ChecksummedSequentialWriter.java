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
package org.apache.cassandra.io.util;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Optional;

public class ChecksummedSequentialWriter extends SequentialWriter
{
    private static final SequentialWriterOption CRC_WRITER_OPTION = SequentialWriterOption.newBuilder()
                                                                                          .bufferSize(8 * 1024)
                                                                                          .build();

    private final SequentialWriter crcWriter;
    private final ChecksumWriter crcMetadata;
    private final Optional<File> digestFile;

    public ChecksummedSequentialWriter(File file, File crcPath, File digestFile, SequentialWriterOption option)
    {
        super(file, option);
        crcWriter = new SequentialWriter(crcPath, CRC_WRITER_OPTION);
        crcMetadata = new ChecksumWriter(crcWriter);
        crcMetadata.writeChunkSize(buffer.capacity());
        this.digestFile = Optional.ofNullable(digestFile);
    }

    @Override
    protected void flushData()
    {
        super.flushData();
        ByteBuffer toAppend = buffer.duplicate();
        toAppend.position(0);
        toAppend.limit(buffer.position());
        crcMetadata.appendDirect(toAppend, false);
    }

    protected class TransactionalProxy extends SequentialWriter.TransactionalProxy
    {
        @Override
        protected Throwable doCommit(Throwable accumulate)
        {
            return super.doCommit(crcWriter.commit(accumulate));
        }

        @Override
        protected Throwable doAbort(Throwable accumulate)
        {
            return super.doAbort(crcWriter.abort(accumulate));
        }

        @Override
        protected void doPrepare()
        {
            syncInternal();
            digestFile.ifPresent(crcMetadata::writeFullChecksum);
            crcWriter.prepareToCommit();
        }
    }

    @Override
    protected SequentialWriter.TransactionalProxy txnProxy()
    {
        return new TransactionalProxy();
    }
}
