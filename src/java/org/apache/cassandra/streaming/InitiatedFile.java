package org.apache.cassandra.streaming;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.cassandra.io.ICompactSerializer;

class InitiatedFile
{
    private static ICompactSerializer<InitiatedFile> serializer_;

    static
    {
        serializer_ = new InitiatedFileSerializer();
    }

    public static ICompactSerializer<InitiatedFile> serializer()
    {
        return serializer_;
    }

    private String targetFile_;
    private long expectedBytes_;
    private String table_;

    public InitiatedFile(String targetFile, long expectedBytes, String table)
    {
        targetFile_ = targetFile;
        expectedBytes_ = expectedBytes;
        table_ = table;
    }

    public String getTable()
    {
        return table_;
    }

    public String getTargetFile()
    {
        return targetFile_;
    }

    public void setTargetFile(String file)
    {
        targetFile_ = file;
    }

    public long getExpectedBytes()
    {
        return expectedBytes_;
    }

    public boolean equals(Object o)
    {
        if ( !(o instanceof InitiatedFile) )
            return false;

        InitiatedFile rhs = (InitiatedFile)o;
        return targetFile_.equals(rhs.targetFile_);
    }

    public int hashCode()
    {
        return toString().hashCode();
    }

    public String toString()
    {
        return targetFile_ + ":" + expectedBytes_;
    }

    private static class InitiatedFileSerializer implements ICompactSerializer<InitiatedFile>
    {
        public void serialize(InitiatedFile sc, DataOutputStream dos) throws IOException
        {
            dos.writeUTF(sc.targetFile_);
            dos.writeLong(sc.expectedBytes_);
            dos.writeUTF(sc.table_);
        }

        public InitiatedFile deserialize(DataInputStream dis) throws IOException
        {
            String targetFile = dis.readUTF();
            long expectedBytes = dis.readLong();
            String table = dis.readUTF();
            return new InitiatedFile(targetFile, expectedBytes, table);
        }
    }
}
