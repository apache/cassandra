package org.apache.cassandra.db;

import java.util.Comparator;

class FileStructComparator implements Comparator<FileStruct>
{
    public int compare(FileStruct f, FileStruct f2)
    {
        return f.reader_.getFileName().compareTo(f2.reader_.getFileName());
    }

    public boolean equals(Object o)
    {
        if (!(o instanceof FileStructComparator))
            return false;
        return true;
    }
}