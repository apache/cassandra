package org.apache.cassandra.io.util;

import java.io.DataInput;
import java.io.IOException;
import java.io.Closeable;

public interface FileDataInput extends DataInput, Closeable
{
    public String getPath();

    public boolean isEOF() throws IOException;

    public void mark();

    public void reset() throws IOException;

    public int bytesPastMark();
}
