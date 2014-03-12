package org.apache.cassandra.io.util;

import java.io.File;

import org.apache.cassandra.io.sstable.Descriptor;

public class ChecksummedSequentialWriter extends SequentialWriter
{
    private final SequentialWriter crcWriter;
    private final DataIntegrityMetadata.ChecksumWriter crcMetadata;

    public ChecksummedSequentialWriter(File file, int bufferSize, boolean skipIOCache, File crcPath)
    {
        super(file, bufferSize, skipIOCache);
        crcWriter = new SequentialWriter(crcPath, 8 * 1024, true);
        crcMetadata = new DataIntegrityMetadata.ChecksumWriter(crcWriter.stream);
        crcMetadata.writeChunkSize(buffer.length);
    }

    protected void flushData()
    {
        super.flushData();
        crcMetadata.append(buffer, 0, validBufferBytes);
    }

    public void writeFullChecksum(Descriptor descriptor)
    {
        crcMetadata.writeFullChecksum(descriptor);
    }

    public void close()
    {
        super.close();
        crcWriter.close();
    }
}
