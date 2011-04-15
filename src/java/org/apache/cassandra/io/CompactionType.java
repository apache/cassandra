package org.apache.cassandra.io;

public enum CompactionType
{
    MAJOR("Major"),
    MINOR("Minor"),
    VALIDATION("Validation"),
    KEY_CACHE_SAVE("Key cache save"),
    ROW_CACHE_SAVE("Row cache save"),
    CLEANUP("Cleanup"),
    SCRUB("Scrub"),
    INDEX_BUILD("Secondary index build"),
    SSTABLE_BUILD("SSTable build"),
    UNKNOWN("Unkown compaction type");

    private final String type;

    CompactionType(String type)
    {
        this.type = type;
    }

    public String toString()
    {
        return type;
    }
}
