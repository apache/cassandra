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
     * Appends the buffer to the the underlying SequenceFile.
     * @param buffer buffer which contains the serialized data.
     * @throws java.io.IOException
     */
    public abstract void append(DataOutputBuffer buffer) throws IOException;

    /**
     * Appends the key and the value to the the underlying SequenceFile.
     * @param keyBuffer buffer which contains the serialized key.
     * @param buffer buffer which contains the serialized data.
     * @throws java.io.IOException
     */
    public abstract void append(DataOutputBuffer keyBuffer, DataOutputBuffer buffer) throws IOException;

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
     * Appends the key and the long value to the the underlying SequenceFile.
     * This is used in the contruction of the index file associated with a
     * SSTable.
     * @param key key associated with this peice of data.
     * @param value value associated with this key.
     * @throws java.io.IOException
     */
    public abstract void append(String key, long value) throws IOException;

    /**
     * Be extremely careful while using this API. This currently
     * used to write the commit log header in the commit logs.
     * If not used carefully it could completely screw up reads
     * of other key/value pairs that are written.
     *
     * @param bytes serialized version of the commit log header.
     * @throws java.io.IOException
    */
    public abstract long writeDirect(byte[] bytes) throws IOException;

    /**
     * Write a long into the underlying sub system.
     * @param value long to be written
     * @throws java.io.IOException
     */
    public abstract void writeLong(long value) throws IOException;

    /**
     * Close the file which is being used for the write.
     * @throws java.io.IOException
     */
    public abstract void close() throws IOException;

    /**
     * Close the file after appending the passed in footer information.
     * @param footer footer information.
     * @param size size of the footer.
     * @throws java.io.IOException
     */
    public abstract void close(byte[] footer, int size) throws IOException;

    /**
     * @return the size of the file.
     * @throws java.io.IOException
     */
    public abstract long getFileSize() throws IOException;


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

        public void append(DataOutputBuffer buffer) throws IOException
        {
            file_.write(buffer.getData(), 0, buffer.getLength());
        }

        public void append(DataOutputBuffer keyBuffer, DataOutputBuffer buffer) throws IOException
        {
            int keyBufLength = keyBuffer.getLength();
            if (keyBuffer == null || keyBufLength == 0)
                throw new IllegalArgumentException("Key cannot be NULL or of zero length.");

            file_.writeInt(keyBufLength);
            file_.write(keyBuffer.getData(), 0, keyBufLength);

            int length = buffer.getLength();
            file_.writeInt(length);
            file_.write(buffer.getData(), 0, length);
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

        public void append(String key, long value) throws IOException
        {
            if (key == null)
                throw new IllegalArgumentException("Key cannot be NULL.");

            file_.writeUTF(key);
            file_.writeLong(value);
        }

        /**
         * Be extremely careful while using this API. This currently
         * used to write the commit log header in the commit logs.
         * If not used carefully it could completely screw up reads
         * of other key/value pairs that are written.
         *
         * @param bytes the bytes to write
         */
        public long writeDirect(byte[] bytes) throws IOException
        {
            file_.write(bytes);
            return file_.getFilePointer();
        }

        public void writeLong(long value) throws IOException
        {
            file_.writeLong(value);
        }

        public void close() throws IOException
        {
            file_.getChannel().force(true);
            file_.close();
        }

        public void close(byte[] footer, int size) throws IOException
        {
            file_.writeInt(size);
            file_.write(footer, 0, size);
        }

        public String getFileName()
        {
            return filename_;
        }

        public long getFileSize() throws IOException
        {
            return file_.length();
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
    }
}
