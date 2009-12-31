package org.apache.cassandra.io.util;

import java.io.DataInput;
import java.io.IOException;
import java.io.Closeable;

public interface FileDataInput extends DataInput, Closeable
{
    public void seek(long pos) throws IOException;

    public long length() throws IOException;

    public long getFilePointer();

    public String getPath();
}
