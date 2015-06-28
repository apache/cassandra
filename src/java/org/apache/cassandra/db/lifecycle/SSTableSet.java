package org.apache.cassandra.db.lifecycle;

public enum SSTableSet
{
    // returns the "canonical" version of any current sstable, i.e. if an sstable is being replaced and is only partially
    // visible to reads, this sstable will be returned as its original entirety, and its replacement will not be returned
    // (even if it completely replaces it)
    CANONICAL,
    // returns the live versions of all sstables, i.e. including partially written sstables
    LIVE,
    NONCOMPACTING
}