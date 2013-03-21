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
package org.apache.cassandra.streaming;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ning.compress.lzf.LZFOutputStream;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata.ChecksumValidator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throttle;
import org.apache.cassandra.utils.WrappedRunnable;

public class FileStreamTask extends WrappedRunnable
{
    private static final Logger logger = LoggerFactory.getLogger(FileStreamTask.class);

    private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;
    public static final int MAX_CONNECT_ATTEMPTS = 4;

    protected final StreamHeader header;
    protected final InetAddress to;

    // communication socket
    protected Socket socket;
    // socket's output/input stream
    private OutputStream output;
    private OutputStream compressedoutput;
    private DataInputStream input;
    // allocate buffer to use for transfers only once
    private byte[] transferBuffer;
    // outbound global throughput limiter
    protected final Throttle throttle;
    private final StreamReplyVerbHandler handler = new StreamReplyVerbHandler();
    protected final StreamingMetrics metrics;

    public FileStreamTask(StreamHeader header, InetAddress to)
    {
        this.header = header;
        this.to = to;
        this.throttle = new Throttle(toString(), new Throttle.ThroughputFunction()
        {
            /** @return Instantaneous throughput target in bytes per millisecond. */
            public int targetThroughput()
            {
                if (DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec() < 1)
                    // throttling disabled
                    return 0;
                // total throughput
                int totalBytesPerMS = DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec() * 1024 * 1024 / 8 / 1000;
                // per stream throughput (target bytes per MS)
                return totalBytesPerMS / Math.max(1, (int)StreamingMetrics.activeStreamsOutbound.count());
            }
        });
        metrics = StreamingMetrics.get(to);
    }

    public void runMayThrow() throws IOException
    {
        try
        {
            connectAttempt();
            // successfully connected: stream.
            // (at this point, if we fail, it is the receiver's job to re-request)
            stream();

            StreamOutSession session = StreamOutSession.get(header.sessionId);
            if (session == null)
            {
                logger.info("Found no stream out session at end of file stream task - this is expected if the receiver went down");
            }
            else if (session.getFiles().size() == 0)
            {
                // we are the last of our kind, receive the final confirmation before closing
                receiveReply();
                logger.info("Finished streaming session to {}", to);
            }
        }
        catch (IOException e)
        {
            StreamOutSession session = StreamOutSession.get(header.sessionId);
            if (session != null)
                session.close(false);
            throw e;
        }
        finally
        {
            try
            {
                close();
            }
            catch (IOException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("error closing socket", e);
            }
        }
        if (logger.isDebugEnabled())
            logger.debug("Done streaming " + header.file);
    }

    /**
     * Stream file by it's sections specified by this.header
     * @throws IOException on any I/O error
     */
    protected void stream() throws IOException
    {
        ByteBuffer headerBuffer = MessagingService.instance().constructStreamHeader(header, false, MessagingService.instance().getVersion(to));
        // write header (this should not be compressed for compatibility with other messages)
        output.write(ByteBufferUtil.getArray(headerBuffer));

        if (header.file == null)
            return;

        // try to skip kernel page cache if possible
        RandomAccessReader file = RandomAccessReader.open(new File(header.file.getFilename()));
        Descriptor desc = Descriptor.fromFilename(header.file.getFilename());
        ChecksumValidator metadata = null;
        if (new File(desc.filenameFor(Component.CRC)).exists())
            metadata = DataIntegrityMetadata.checksumValidator(desc);
        transferBuffer = metadata == null ? new byte[DEFAULT_CHUNK_SIZE] : new byte[metadata.chunkSize];

        // setting up data compression stream
        compressedoutput = new LZFOutputStream(output);

        StreamingMetrics.activeStreamsOutbound.inc();
        try
        {
            long totalBytesTransferred = 0;
            // stream each of the required sections of the file
            for (Pair<Long, Long> section : header.file.sections)
            {
                long start = metadata == null ? section.left : metadata.chunkStart(section.left);
                int skipBytes = (int) (section.left - start);
                // seek to the beginning of the section
                file.seek(start);
                if (metadata != null)
                    metadata.seek(start);

                // length of the section to read
                long length = section.right - start;
                // tracks write progress
                long bytesTransferred = 0;

                while (bytesTransferred < length)
                {
                    long lastWrite = write(file, metadata, skipBytes, length, bytesTransferred);
                    bytesTransferred += lastWrite;
                    totalBytesTransferred += lastWrite;
                    // store streaming progress
                    header.file.progress += lastWrite;
                    skipBytes = 0;
                }

                // make sure that current section is send
                compressedoutput.flush();

                if (logger.isDebugEnabled())
                    logger.debug("Bytes transferred " + bytesTransferred + "/" + header.file.size);
            }
            StreamingMetrics.totalOutgoingBytes.inc(totalBytesTransferred);
            metrics.outgoingBytes.inc(totalBytesTransferred);
            // receive reply confirmation
            receiveReply();
        }
        finally
        {
            StreamingMetrics.activeStreamsOutbound.dec();

            // no matter what happens close file
            FileUtils.closeQuietly(file);
        }
    }

    public static void sendReply(MessageOut message, DataOutputStream out) throws IOException
    {
        out.writeInt(MessagingService.PROTOCOL_MAGIC);
        message.serialize(out, MessagingService.current_version);
    }

    protected void receiveReply() throws IOException
    {
        MessagingService.validateMagic(input.readInt());
        // since we reject streaming with different version, using current_version here is fine
        MessageIn message = MessageIn.read(input, MessagingService.current_version, -1);
        assert message.verb == MessagingService.Verb.STREAM_REPLY : "Non-reply message received on stream socket";
        handler.doVerb(message, -1);
    }

    /**
     * Sequentially read bytes from the file and write them to the output stream
     *
     * @param reader The file reader to read from
     * @param validator validator to verify data integrity
     * @param start number of bytes to skip transfer, but include for validation.
     * @param length The full length that should be transferred
     * @param bytesTransferred Number of bytes remaining to transfer
     *
     * @return Number of bytes transferred
     *
     * @throws IOException on any I/O error
     */
    protected long write(RandomAccessReader reader, ChecksumValidator validator, int start, long length, long bytesTransferred) throws IOException
    {
        int toTransfer = (int) Math.min(transferBuffer.length, length - bytesTransferred);
        int minReadable = (int) Math.min(transferBuffer.length, reader.length() - reader.getFilePointer());

        reader.readFully(transferBuffer, 0, minReadable);
        if (validator != null)
            validator.validate(transferBuffer, 0, minReadable);

        compressedoutput.write(transferBuffer, start, (toTransfer - start));
        throttle.throttleDelta(toTransfer);

        return toTransfer;
    }

    /**
     * Connects to the destination, with backoff for failed attempts.
     * TODO: all nodes on a cluster must currently use the same storage port
     * @throws IOException If all attempts fail.
     */
    private void connectAttempt() throws IOException
    {
        int attempts = 0;
        while (true)
        {
            try
            {
                socket = MessagingService.instance().getConnectionPool(to).newSocket();
                socket.setSoTimeout(DatabaseDescriptor.getStreamingSocketTimeout());
                output = socket.getOutputStream();
                input = new DataInputStream(socket.getInputStream());
                break;
            }
            catch (IOException e)
            {
                if (++attempts >= MAX_CONNECT_ATTEMPTS)
                    throw e;

                long waitms = DatabaseDescriptor.getRpcTimeout() * (long)Math.pow(2, attempts);
                logger.warn("Failed attempt " + attempts + " to connect to " + to + " to stream " + header.file + ". Retrying in " + waitms + " ms. (" + e + ")");
                try
                {
                    Thread.sleep(waitms);
                }
                catch (InterruptedException wtf)
                {
                    throw new RuntimeException(wtf);
                }
            }
        }
    }

    protected void close() throws IOException
    {
        if (output != null)
            output.close();
    }

    public String toString()
    {
        return String.format("FileStreamTask(session=%s, to=%s)", header.sessionId, to);
    }
}
