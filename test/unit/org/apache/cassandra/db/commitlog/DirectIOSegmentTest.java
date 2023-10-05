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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.BeforeClass;
import org.junit.Test;

import net.openhft.chronicle.core.util.ThrowingFunction;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SimpleCachedBufferPool;
import org.apache.cassandra.utils.Generators;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.creation.MockSettingsImpl;
import sun.nio.ch.DirectBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.quicktheories.QuickTheory.qt;

public class DirectIOSegmentTest
{
    @BeforeClass
    public static void beforeClass() throws IOException
    {
        File commitLogDir = new File(Files.createTempDirectory("commitLogDir"));
        DatabaseDescriptor.daemonInitialization(() -> {
            Config config = DatabaseDescriptor.loadConfig();
            config.commitlog_directory = commitLogDir.toString();
            return config;
        });
    }

    @Test
    public void testFlushBuffer()
    {
        int fsBlockSize = 32;
        int bufSize = 4 * fsBlockSize;

        SimpleCachedBufferPool bufferPool = mock(SimpleCachedBufferPool.class);
        AbstractCommitLogSegmentManager manager = mock(AbstractCommitLogSegmentManager.class,
                                                       new MockSettingsImpl<>().useConstructor(CommitLog.instance, DatabaseDescriptor.getCommitLogLocation()));
        doReturn(bufferPool).when(manager).getBufferPool();
        doCallRealMethod().when(manager).getConfiguration();
        when(bufferPool.createBuffer()).thenReturn(ByteBuffer.allocate(bufSize + fsBlockSize));
        doNothing().when(manager).addSize(anyLong());

        qt().forAll(Generators.forwardRanges(0, bufSize))
            .checkAssert(startEnd -> {
                int start = startEnd.lowerEndpoint();
                int end = startEnd.upperEndpoint();
                FileChannel channel = mock(FileChannel.class);
                ThrowingFunction<Path, FileChannel, IOException> channelFactory = path -> channel;
                ArgumentCaptor<ByteBuffer> bufCap = ArgumentCaptor.forClass(ByteBuffer.class);
                DirectIOSegment seg = new DirectIOSegment(manager, channelFactory, fsBlockSize);
                seg.lastSyncedOffset = start;
                seg.flush(start, end);
                try
                {
                    verify(channel).write(bufCap.capture());
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
                ByteBuffer buf = bufCap.getValue();

                // assert that the entire buffer is written
                assertThat(buf.position()).isLessThanOrEqualTo(start);
                assertThat(buf.limit()).isGreaterThanOrEqualTo(end);

                // assert that the buffer is aligned to the fs block size
                assertThat(buf.position() % fsBlockSize).isZero();
                assertThat(buf.limit() % fsBlockSize).isZero();

                // assert that the buffer is unnecessarily large
                assertThat(buf.position()).isGreaterThan(start - fsBlockSize);
                assertThat(buf.limit()).isLessThan(end + fsBlockSize);

                assertThat(seg.lastWritten).isEqualTo(buf.limit());
            });
    }

    @Test
    public void testFlushSize()
    {
        int fsBlockSize = 32;
        int bufSize = 4 * fsBlockSize;

        SimpleCachedBufferPool bufferPool = mock(SimpleCachedBufferPool.class);
        AbstractCommitLogSegmentManager manager = mock(AbstractCommitLogSegmentManager.class,
                                                       new MockSettingsImpl<>().useConstructor(CommitLog.instance, DatabaseDescriptor.getCommitLogLocation()));
        doReturn(bufferPool).when(manager).getBufferPool();
        doCallRealMethod().when(manager).getConfiguration();
        when(bufferPool.createBuffer()).thenReturn(ByteBuffer.allocate(bufSize + fsBlockSize));
        doNothing().when(manager).addSize(anyLong());

        FileChannel channel = mock(FileChannel.class);
        ThrowingFunction<Path, FileChannel, IOException> channelFactory = path -> channel;
        ArgumentCaptor<ByteBuffer> bufCap = ArgumentCaptor.forClass(ByteBuffer.class);
        DirectIOSegment seg = new DirectIOSegment(manager, channelFactory, fsBlockSize);

        AtomicLong size = new AtomicLong();
        doAnswer(i -> size.addAndGet(i.getArgument(0, Long.class))).when(manager).addSize(anyLong());

        for (int start = 0; start < bufSize - 1; start++)
        {
            int end = start + 1;
            seg.lastSyncedOffset = start;
            seg.flush(start, end);
            assertThat(size.get()).isGreaterThanOrEqualTo(end);
        }

        assertThat(size.get()).isEqualTo(bufSize);
    }

    @Test
    public void testBuilder()
    {
        AbstractCommitLogSegmentManager manager = mock(AbstractCommitLogSegmentManager.class,
                                                       new MockSettingsImpl<>().useConstructor(CommitLog.instance, DatabaseDescriptor.getCommitLogLocation()));
        DirectIOSegment.DirectIOSegmentBuilder builder = new DirectIOSegment.DirectIOSegmentBuilder(manager, 4096);
        assertThat(builder.fsBlockSize).isGreaterThan(0);

        int segmentSize = Math.max(5 << 20, builder.fsBlockSize * 5);
        DatabaseDescriptor.setCommitLogSegmentSize(segmentSize >> 20);

        SimpleCachedBufferPool pool = builder.createBufferPool();
        ByteBuffer buf = pool.createBuffer();
        try
        {
            assertThat(buf.remaining()).isEqualTo(segmentSize);
            assertThat(buf.alignmentOffset(buf.position(), builder.fsBlockSize)).isEqualTo(0);
            assertThat(buf).isInstanceOf(DirectBuffer.class);
            assertThat(((DirectBuffer) buf).attachment()).isNotNull();
        }
        finally
        {
            pool.releaseBuffer(buf);
        }
    }
}