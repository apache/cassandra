package org.apache.cassandra.db.commitlog;

import java.io.DataInput;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileSegmentInputStream;

/**
 * Each segment of an encrypted file may contain many encrypted chunks, and each chunk needs to be individually decrypted
 * to reconstruct the full segment.
 */
public class EncryptedFileSegmentInputStream extends FileSegmentInputStream implements FileDataInput, DataInput
{
    private final long segmentOffset;
    private final int expectedLength;
    private final ChunkProvider chunkProvider;

    /**
     * offset the decrypted chunks already processed in this segment.
     */
    private int totalChunkOffset;

    public EncryptedFileSegmentInputStream(String filePath, long segmentOffset, int position, int expectedLength, ChunkProvider chunkProvider)
    {
        super(chunkProvider.nextChunk(), filePath, position);
        this.segmentOffset = segmentOffset;
        this.expectedLength = expectedLength;
        this.chunkProvider = chunkProvider;
    }

    public interface ChunkProvider
    {
        /**
         * Get the next chunk from the backing provider, if any chunks remain.
         * @return Next chunk, else null if no more chunks remain.
         */
        ByteBuffer nextChunk();
    }

    public long getFilePointer()
    {
        return segmentOffset + totalChunkOffset + buffer.position();
    }

    public boolean isEOF()
    {
        return totalChunkOffset + buffer.position() >= expectedLength;
    }

    public long bytesRemaining()
    {
        return expectedLength - (totalChunkOffset + buffer.position());
    }

    public void seek(long position)
    {
        // implement this when we actually need it
        throw new UnsupportedOperationException();
    }

    public long bytesPastMark(DataPosition mark)
    {
        throw new UnsupportedOperationException();
    }

    public void reBuffer()
    {
        totalChunkOffset += buffer.position();
        buffer = chunkProvider.nextChunk();
    }
}
