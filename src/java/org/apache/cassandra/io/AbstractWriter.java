package org.apache.cassandra.io;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;

public abstract class AbstractWriter
{
    protected String filename_;

    AbstractWriter(String filename)
    {
        filename_ = filename;
    }

    public String getFileName()
    {
        return filename_;
    }

    public long lastModified()
    {
        File file = new File(filename_);
        return file.lastModified();
    }

    /**
     * Get the current position of the file pointer.
     * @return current file pointer position
     * @throws java.io.IOException
     */
    public abstract long getCurrentPosition() throws IOException;

    /**
     * Seeks the file pointer to the specified position.
     * @param position position within the file to seek to.
     * @throws java.io.IOException
     */
    public abstract void seek(long position) throws IOException;

    /**
     * Appends the key and the value to the the underlying SequenceFile.
     * @param key key associated with this peice of data.
     * @param buffer buffer containing the serialized data.
     * @throws java.io.IOException
     */
    public abstract void append(String key, DataOutputBuffer buffer) throws IOException;

    /**
     * Appends the key and the value to the the underlying SequenceFile.
     * @param key key associated with this peice of data.
     * @param value byte array containing the serialized data.
     * @throws java.io.IOException
     */
    public abstract void append(String key, byte[] value) throws IOException;

    /**
     * Close the file which is being used for the write.
     * @throws java.io.IOException
     */
    public abstract void close() throws IOException;

    /**
     * @return the size of the file.
     * @throws java.io.IOException
     */
    public abstract long getFileSize() throws IOException;

    /** fsync the writer */
    public abstract void sync() throws IOException;


    public static class Writer extends AbstractWriter
    {
        protected RandomAccessFile file_;

        Writer(String filename) throws IOException
        {
            super(filename);
            init(filename);
        }

        Writer(String filename, int size) throws IOException
        {
            super(filename);
            init(filename, size);
        }

        protected void init(String filename) throws IOException
        {
            File file = new File(filename);
            if (!file.exists())
            {
                file.createNewFile();
            }
            file_ = new RandomAccessFile(file, "rw");
        }

        protected void init(String filename, int size) throws IOException
        {
            init(filename);
        }

        public long getCurrentPosition() throws IOException
        {
            return file_.getFilePointer();
        }

        public void seek(long position) throws IOException
        {
            file_.seek(position);
        }

        public void append(String key, DataOutputBuffer buffer) throws IOException
        {
            if (key == null)
                throw new IllegalArgumentException("Key cannot be NULL.");

            file_.writeUTF(key);
            int length = buffer.getLength();
            file_.writeInt(length);
            file_.write(buffer.getData(), 0, length);
        }

        public void append(String key, byte[] value) throws IOException
        {
            if (key == null)
                throw new IllegalArgumentException("Key cannot be NULL.");

            file_.writeUTF(key);
            file_.writeInt(value.length);
            file_.write(value);
        }

        public void close() throws IOException
        {
            sync();
            file_.close();
        }

        public String getFileName()
        {
            return filename_;
        }

        public long getFileSize() throws IOException
        {
            return file_.length();
        }

        public void sync() throws IOException
        {
            file_.getChannel().force(true);
        }
    }

    public static class BufferWriter extends Writer
    {

        BufferWriter(String filename, int size) throws IOException
        {
            super(filename, size);
        }

        @Override
        protected void init(String filename) throws IOException
        {
            init(filename, 0);
        }

        @Override
        protected void init(String filename, int size) throws IOException
        {
            File file = new File(filename);
            file_ = new BufferedRandomAccessFile(file, "rw", size);
            if (!file.exists())
            {
                file.createNewFile();
            }
        }

        @Override
        public void sync() throws IOException
        {
            ((BufferedRandomAccessFile)file_).sync();
        }
    }
}
