package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOError;
import java.io.IOException;

public class BufferedSegmentedFile extends SegmentedFile
{
    public BufferedSegmentedFile(String path, long length)
    {
        super(path, length);
    }

    public static class Builder extends SegmentedFile.Builder
    {
        /**
         * Adds a position that would be a safe place for a segment boundary in the file. For a block/row based file
         * format, safe boundaries are block/row edges.
         * @param boundary The absolute position of the potential boundary in the file.
         */
        public void addPotentialBoundary(long boundary)
        {
            // only one segment in a standard-io file
        }

        /**
         * Called after all potential boundaries have been added to apply this Builder to a concrete file on disk.
         * @param path The file on disk.
         */
        public SegmentedFile complete(String path)
        {
            long length = new File(path).length();
            return new BufferedSegmentedFile(path, length);
        }
    }

    public FileDataInput getSegment(long position, int bufferSize)
    {
        try
        {
            BufferedRandomAccessFile file = new BufferedRandomAccessFile(path, "r", bufferSize);
            file.seek(position);
            return file;
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }
}
