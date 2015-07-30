package org.apache.cassandra.db.transform;

// a shared internal interface, that is hidden to provide type-safety to the user
interface MoreContents<I>
{
    public abstract I moreContents();
}

