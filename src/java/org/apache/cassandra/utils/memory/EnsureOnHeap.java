package org.apache.cassandra.utils.memory;

import java.util.Iterator;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.utils.SearchIterator;

public abstract class EnsureOnHeap extends Transformation
{
    public abstract DecoratedKey applyToPartitionKey(DecoratedKey key);
    public abstract UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition);
    public abstract SearchIterator<Clustering, Row> applyToPartition(SearchIterator<Clustering, Row> partition);
    public abstract Iterator<Row> applyToPartition(Iterator<Row> partition);
    public abstract DeletionInfo applyToDeletionInfo(DeletionInfo deletionInfo);
    public abstract Row applyToRow(Row row);
    public abstract Row applyToStatic(Row row);
    public abstract RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker);

    static class CloneToHeap extends EnsureOnHeap
    {
        protected BaseRowIterator<?> applyToPartition(BaseRowIterator partition)
        {
            return partition instanceof UnfilteredRowIterator
                   ? Transformation.apply((UnfilteredRowIterator) partition, this)
                   : Transformation.apply((RowIterator) partition, this);
        }

        public DecoratedKey applyToPartitionKey(DecoratedKey key)
        {
            return new BufferDecoratedKey(key.getToken(), HeapAllocator.instance.clone(key.getKey()));
        }

        public Row applyToRow(Row row)
        {
            if (row == null)
                return null;
            return Rows.copy(row, HeapAllocator.instance.cloningBTreeRowBuilder()).build();
        }

        public Row applyToStatic(Row row)
        {
            if (row == Rows.EMPTY_STATIC_ROW)
                return row;
            return applyToRow(row);
        }

        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            return marker.copy(HeapAllocator.instance);
        }

        public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            return Transformation.apply(partition, this);
        }

        public SearchIterator<Clustering, Row> applyToPartition(SearchIterator<Clustering, Row> partition)
        {
            return new SearchIterator<Clustering, Row>()
            {
                public boolean hasNext()
                {
                    return partition.hasNext();
                }

                public Row next(Clustering key)
                {
                    return applyToRow(partition.next(key));
                }
            };
        }

        public Iterator<Row> applyToPartition(Iterator<Row> partition)
        {
            return new Iterator<Row>()
            {
                public boolean hasNext()
                {
                    return partition.hasNext();
                }
                public Row next()
                {
                    return applyToRow(partition.next());
                }
                public void remove()
                {
                    partition.remove();
                }
            };
        }

        public DeletionInfo applyToDeletionInfo(DeletionInfo deletionInfo)
        {
            return deletionInfo.copy(HeapAllocator.instance);
        }
    }

    static class NoOp extends EnsureOnHeap
    {
        protected BaseRowIterator<?> applyToPartition(BaseRowIterator partition)
        {
            return partition;
        }

        public DecoratedKey applyToPartitionKey(DecoratedKey key)
        {
            return key;
        }

        public Row applyToRow(Row row)
        {
            return row;
        }

        public Row applyToStatic(Row row)
        {
            return row;
        }

        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            return marker;
        }

        public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            return partition;
        }

        public SearchIterator<Clustering, Row> applyToPartition(SearchIterator<Clustering, Row> partition)
        {
            return partition;
        }

        public Iterator<Row> applyToPartition(Iterator<Row> partition)
        {
            return partition;
        }

        public DeletionInfo applyToDeletionInfo(DeletionInfo deletionInfo)
        {
            return deletionInfo;
        }
    }
}
