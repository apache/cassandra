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

package org.apache.cassandra.net;

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DataRateSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamingDataOutputPlusFixed;
import org.apache.cassandra.utils.FBUtilities;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.ReferenceCountUtil;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AsyncStreamingOutputPlusTest
{

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static SelfSignedCertificate ssc;

    private DataRateSpec.LongBytesPerSecondBound entireSSTableThroughput;
    private DataRateSpec.LongBytesPerSecondBound entireSSTableInterDCThroughput;

    @BeforeClass
    public static void generateCert() throws CertificateException
    {
        ssc = new SelfSignedCertificate();
    }

    @AfterClass
    public static void deleteCert()
    {
        if (ssc != null)
            ssc.delete();
    }

    /**
     * Several tests here have to turn entire-sstable throttling off to reach the branch they are about, and the
     * limiters are static, so without this the last one to run leaves its rate behind for every other test in the
     * JVM. Saved before anything can throw so a failure cannot make the restore invent a default.
     */
    @Before
    public void saveThroughputConfig()
    {
        entireSSTableThroughput = DatabaseDescriptor.getRawConfig().entire_sstable_stream_throughput_outbound;
        entireSSTableInterDCThroughput = DatabaseDescriptor.getRawConfig().entire_sstable_inter_dc_stream_throughput_outbound;
    }

    @After
    public void restoreThroughputConfig()
    {
        DatabaseDescriptor.getRawConfig().entire_sstable_stream_throughput_outbound = entireSSTableThroughput;
        DatabaseDescriptor.getRawConfig().entire_sstable_inter_dc_stream_throughput_outbound = entireSSTableInterDCThroughput;
        StreamManager.StreamRateLimiter.updateEntireSSTableThroughput();
        StreamManager.StreamRateLimiter.updateEntireSSTableInterDCThroughput();
    }

    /**
     * Entire-sstable streaming is throttled to 24MiB/s out of the box, so {@code isRateLimited()} is true by default
     * and the sendfile branch that hands netty a single {@link io.netty.channel.DefaultFileRegion} is not reached at
     * all until both rates are zero.
     */
    private static StreamManager.StreamRateLimiter unthrottledEntireSSTableLimiter()
    {
        DatabaseDescriptor.setEntireSSTableStreamThroughputOutboundMebibytesPerSec(0);
        DatabaseDescriptor.setEntireSSTableInterDCStreamThroughputOutboundMebibytesPerSec(0);
        StreamManager.StreamRateLimiter.updateEntireSSTableThroughput();
        StreamManager.StreamRateLimiter.updateEntireSSTableInterDCThroughput();

        StreamManager.StreamRateLimiter limiter =
            StreamManager.getEntireSSTableRateLimiter(FBUtilities.getBroadcastAddressAndPort());
        assertFalse("the unthrottled sendfile branch is unreachable while the limiter is rate limited",
                    limiter.isRateLimited());
        return limiter;
    }

    @Test
    public void testSuccess() throws IOException
    {
        EmbeddedChannel channel = new TestChannel(4);
        ByteBuf read;
        try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel))
        {
            out.writeInt(1);
            assertEquals(0, out.flushed());
            assertEquals(0, out.flushedToNetwork());
            assertEquals(4, out.position());

            out.doFlush(0);
            assertEquals(4, out.flushed());
            assertEquals(4, out.flushedToNetwork());

            out.writeInt(2);
            assertEquals(8, out.position());
            assertEquals(4, out.flushed());
            assertEquals(4, out.flushedToNetwork());

            out.doFlush(0);
            assertEquals(8, out.position());
            assertEquals(8, out.flushed());
            assertEquals(4, out.flushedToNetwork());

            read = channel.readOutbound();
            assertEquals(4, read.readableBytes());
            assertEquals(1, read.getInt(0));
            assertEquals(8, out.flushed());
            assertEquals(8, out.flushedToNetwork());

            read = channel.readOutbound();
            assertEquals(4, read.readableBytes());
            assertEquals(2, read.getInt(0));

            out.write(new byte[16]);
            assertEquals(24, out.position());
            assertEquals(8, out.flushed());
            assertEquals(8, out.flushedToNetwork());

            out.doFlush(0);
            assertEquals(24, out.position());
            assertEquals(24, out.flushed());
            assertEquals(24, out.flushedToNetwork());

            read = channel.readOutbound();
            assertEquals(16, read.readableBytes());
            assertEquals(0, read.getLong(0));
            assertEquals(0, read.getLong(8));
            assertEquals(24, out.position());
            assertEquals(24, out.flushed());
            assertEquals(24, out.flushedToNetwork());

            out.writeToChannel(alloc -> {
                ByteBuffer buffer = alloc.get(16);
                buffer.putLong(1);
                buffer.putLong(2);
                buffer.flip();
            }, StreamManager.getRateLimiter(FBUtilities.getBroadcastAddressAndPort()));

            assertEquals(40, out.position());
            assertEquals(40, out.flushed());
            assertEquals(40, out.flushedToNetwork());

            read = channel.readOutbound();
            assertEquals(16, read.readableBytes());
            assertEquals(1, read.getLong(0));
            assertEquals(2, read.getLong(8));
        }
    }

    @Test
    public void testWriteFileToChannelEntireSSTableNoThrottling() throws IOException
    {
        unthrottledEntireSSTableLimiter();

        testWriteFileToChannel(true);
    }

    @Test
    public void testWriteFileToChannelEntireSSTable() throws IOException
    {
        // Enable entire SSTable throttling by setting it to 200 Mbps
        DatabaseDescriptor.setEntireSSTableStreamThroughputOutboundMebibytesPerSec(200);
        DatabaseDescriptor.setEntireSSTableInterDCStreamThroughputOutboundMebibytesPerSec(200);
        StreamManager.StreamRateLimiter.updateEntireSSTableThroughput();
        StreamManager.StreamRateLimiter.updateEntireSSTableInterDCThroughput();

        testWriteFileToChannel(true);
    }

    @Test
    public void testWriteFileToChannelSSL() throws IOException
    {
        testWriteFileToChannel(false);
    }

    private void testWriteFileToChannel(boolean zeroCopy) throws IOException
    {
        File file = populateTempData("zero_copy_" + zeroCopy);
        int length = (int) file.length();

        EmbeddedChannel channel = new TestChannel(4);
        StreamManager.StreamRateLimiter limiter = zeroCopy ? StreamManager.getEntireSSTableRateLimiter(FBUtilities.getBroadcastAddressAndPort())
                                                           : StreamManager.getRateLimiter(FBUtilities.getBroadcastAddressAndPort());

        try (FileChannel fileChannel = file.newReadChannel();
             AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel))
        {
            assertTrue(fileChannel.isOpen());

            if (zeroCopy)
                out.writeFileToChannelZeroCopy(fileChannel, limiter, length, length, length * 2);
            else
                out.writeFileToChannel(fileChannel, limiter, length);

            assertEquals(length, out.flushed());
            assertEquals(length, out.flushedToNetwork());
            assertEquals(length, out.position());

            assertFalse(fileChannel.isOpen());
        }
    }

    /**
     * The ranged {@code writeFileToChannel} over an encrypted connection. An {@link SslHandler} in the pipeline
     * selects the batching branch instead of the sendfile one, and that branch has to read from {@code position}
     * and keep advancing from it across batches; reading from 0 would send the wrong bytes with no error anywhere.
     * <p>
     * Note that {@link #testWriteFileToChannelSSL} does not actually install an SslHandler, so without this the
     * encrypted branch is never executed at all.
     */
    @Test(timeout = 60_000)
    public void testWriteFileToChannelRangedOverSsl() throws Exception
    {
        byte[] content = new byte[200_000];
        new Random(0).nextBytes(content);
        File file = populateTempData("ssl_ranged", content);

        long position = 12_345;
        long length = 100_000;   // 65536 + 34464, so the second batch is partial and starts mid-file

        SslContext serverCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
        SslContext clientCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        EmbeddedChannel client = new EmbeddedChannel(clientCtx.newHandler(ByteBufAllocator.DEFAULT));
        EmbeddedChannel server = new EmbeddedChannel(serverCtx.newHandler(ByteBufAllocator.DEFAULT));

        try
        {
            // Has to finish before the first application write, or the writer parks waiting for writability.
            completeHandshake(client, server);
            assertNotNull(client.pipeline().get(SslHandler.class));

            // Added at the tail, so outbound writes reach it before the SslHandler encrypts them: it sees one
            // plaintext ByteBuf per batch, and would see a FileRegion if the branch selection ever inverted.
            AtomicInteger batches = new AtomicInteger();
            client.pipeline().addLast(new ChannelOutboundHandlerAdapter()
            {
                @Override
                public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
                {
                    assertTrue("the encrypted path must not hand a FileRegion to the SslHandler: " + msg,
                               msg instanceof ByteBuf);
                    batches.incrementAndGet();
                    super.write(ctx, msg, promise);
                }
            });

            FileChannel fileChannel = file.newReadChannel();   // ownership passes to writeFileToChannel
            try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(client))
            {
                long written = out.writeFileToChannel(fileChannel,
                                                      StreamManager.getRateLimiter(FBUtilities.getBroadcastAddressAndPort()),
                                                      position, length);
                assertEquals(length, written);
                assertEquals(length, out.flushed());
            }
            assertFalse("the batching branch closes the channel it was given", fileChannel.isOpen());
            assertEquals(2, batches.get());

            // Decrypt everything the writer produced by feeding it through the peer's SslHandler.
            ByteBuf plaintext = Unpooled.buffer((int) length);
            int cipherBytes = 0;
            Object msg;
            while ((msg = client.readOutbound()) != null)
            {
                ByteBuf encrypted = (ByteBuf) msg;
                cipherBytes += encrypted.readableBytes();
                server.writeInbound(encrypted);              // takes ownership
                ByteBuf decrypted;
                while ((decrypted = server.readInbound()) != null)
                {
                    plaintext.writeBytes(decrypted);
                    decrypted.release();
                }
            }
            assertTrue("nothing was encrypted", cipherBytes > 0);

            byte[] arrived = new byte[plaintext.readableBytes()];
            plaintext.getBytes(0, arrived);
            assertEquals(length, arrived.length);
            assertArrayEquals("the encrypted branch sent the wrong byte range",
                              Arrays.copyOfRange(content, (int) position, (int) (position + length)), arrived);
        }
        finally
        {
            client.finishAndReleaseAll();
            server.finishAndReleaseAll();
        }
    }

    /**
     * The unthrottled sendfile path hands netty a {@link io.netty.channel.DefaultFileRegion}, and netty closing that
     * region is what closes the {@link FileChannel} this method took ownership of -- so if the write never happens,
     * nothing else will. {@code beginFlush} is where that can happen without the caller doing anything wrong: it
     * propagates the failure of any EARLIER flush on the connection, so the first range to be written after a peer
     * disconnects throws before a region exists, and one such failure must not strand the descriptor of every range
     * after it. An entire-sstable stream is one channel per component per range.
     */
    @Test
    public void testUnthrottledWriteFileToChannelClosesFileWhenTheFlushHasAlreadyFailed() throws IOException
    {
        StreamManager.StreamRateLimiter limiter = unthrottledEntireSSTableLimiter();

        File file = populateTempData("zero_copy_flush_failure");
        long length = file.length();

        // Fails every write, without passing it on, which is how the connection dying looks to the writer.
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            {
                ReferenceCountUtil.release(msg);
                promise.setFailure(new IOException("the connection dropped"));
            }
        });

        // Not try-with-resources: close() flushes, and flushing is what rethrows the failed flush.
        AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel);
        FileChannel fileChannel = file.newReadChannel();
        try
        {
            out.writeInt(1);
            out.doFlush(0);          // fails asynchronously; recorded, not thrown, until the next beginFlush

            assertTrue(fileChannel.isOpen());
            try
            {
                out.writeFileToChannel(fileChannel, limiter, 0, length);
                fail("a failed flush on the connection has to be propagated, not written over");
            }
            catch (IOException expected)
            {
                // AsyncChannelOutputPlus.FlushException
            }
            assertFalse("the file descriptor was stranded by a write that never reached netty",
                        fileChannel.isOpen());
        }
        finally
        {
            out.discard();
            fileChannel.close();   // idempotent; the point of the assertion above is that it is already closed
        }
    }

    /**
     * The other implementation of the same contract: {@link StreamingDataOutputPlusFixed}, which
     * {@code NettyStreamingChannel} uses for the small control messages. Tested here for want of a better home --
     * the contract belongs to {@code StreamingDataOutputPlus}, and this is the only test of any implementation of
     * it.
     * <p>
     * Both of the ways this can fail to write a whole range used to return the short count instead of throwing, and
     * a short count is unusable: the peer sizes each component from the component manifest it was already sent, not
     * from the stream, so the bytes missing here are taken out of the NEXT component, shifting every component after
     * it and finally leaving the peer blocked on bytes that never come -- with nothing thrown at either end.
     * <p>
     * The in-JVM dtest implementation in {@code DirectStreamingConnectionFactory} makes the same two promises and
     * cannot be reached from a unit test (it is an anonymous class inside a factory method).
     */
    @Test
    public void testFixedOutputPlusThrowsRatherThanWritingPartOfARange() throws IOException
    {
        StreamManager.StreamRateLimiter limiter =
            StreamManager.getRateLimiter(FBUtilities.getBroadcastAddressAndPort());

        byte[] content = new byte[64];
        new Random(0).nextBytes(content);
        File file = populateTempData("fixed_exact_length", content);

        // A range longer than what is left of the fixed buffer: a 20-byte prefix used to be written and returned.
        FileChannel tooLongForTheBuffer = file.newReadChannel();
        try (StreamingDataOutputPlusFixed out = new StreamingDataOutputPlusFixed(ByteBuffer.allocate(20)))
        {
            assertThatThrownBy(() -> out.writeFileToChannel(tooLongForTheBuffer, limiter, 0, content.length))
                .isInstanceOf(BufferOverflowException.class);
        }
        assertFalse("the channel is owned by the call, whether it succeeds or fails", tooLongForTheBuffer.isOpen());

        // A range that runs off the end of the file: the bytes are not there, and the peer expects all of them.
        FileChannel pastEndOfFile = file.newReadChannel();
        try (StreamingDataOutputPlusFixed out = new StreamingDataOutputPlusFixed(ByteBuffer.allocate(1024)))
        {
            assertThatThrownBy(() -> out.writeFileToChannel(pastEndOfFile, limiter, 32, content.length))
                .isInstanceOf(EOFException.class);
        }
        assertFalse(pastEndOfFile.isOpen());

        // ...and a range that is all there is written whole, from the right offset, and reported as written.
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        FileChannel ranged = file.newReadChannel();
        long written;
        try (StreamingDataOutputPlusFixed out = new StreamingDataOutputPlusFixed(buffer))
        {
            written = out.writeFileToChannel(ranged, limiter, 8, content.length - 8L);
        }
        assertEquals(content.length - 8L, written);
        assertFalse(ranged.isOpen());
        assertArrayEquals("the wrong byte range was written",
                          Arrays.copyOfRange(content, 8, content.length),
                          Arrays.copyOfRange(buffer.array(), 0, content.length - 8));

        // The whole-file form is the ranged one, so it makes the same promises.
        ByteBuffer wholeFileBuffer = ByteBuffer.allocate(1024);
        FileChannel whole = file.newReadChannel();
        try (StreamingDataOutputPlusFixed out = new StreamingDataOutputPlusFixed(wholeFileBuffer))
        {
            assertEquals(content.length, out.writeFileToChannel(whole, limiter));
        }
        assertFalse(whole.isOpen());
        assertArrayEquals(content, Arrays.copyOfRange(wholeFileBuffer.array(), 0, content.length));
    }

    /** Drive both EmbeddedChannels until each SslHandler reports its handshake done. */
    private static void completeHandshake(EmbeddedChannel client, EmbeddedChannel server)
    {
        for (int i = 0; i < 32; i++)
        {
            pump(client, server);
            pump(server, client);
            if (client.pipeline().get(SslHandler.class).handshakeFuture().isDone()
                && server.pipeline().get(SslHandler.class).handshakeFuture().isDone())
            {
                assertTrue(client.pipeline().get(SslHandler.class).handshakeFuture().isSuccess());
                assertTrue(server.pipeline().get(SslHandler.class).handshakeFuture().isSuccess());
                return;
            }
        }
        throw new AssertionError("TLS handshake did not complete");
    }

    private static void pump(EmbeddedChannel from, EmbeddedChannel to)
    {
        Object msg;
        while ((msg = from.readOutbound()) != null)
            to.writeInbound(msg);
        from.runPendingTasks();
        to.runPendingTasks();
    }

    private File populateTempData(String name) throws IOException
    {
        byte[] content = new byte[16];
        new Random().nextBytes(content);
        return populateTempData(name, content);
    }

    private File populateTempData(String name, byte[] content) throws IOException
    {
        File file = new File(Files.createTempFile(name, ".txt"));
        file.deleteOnExit();
        Files.write(file.toPath(), content);
        return file;
    }
}
