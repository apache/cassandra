package org.apache.cassandra.db.virtual;

import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;

final public class CachesTable extends AbstractVirtualTable
{

    private final static String NAME = "name";
    private static final String SIZE_USED = "size_used";
    private static final String HIT_RATIO = "hit_ratio";
    private static final String HITS = "hits";
    private static final String REQUESTS_RATE_15M = "recent_requests_per_sec";
    private static final String REQUESTS = "requests";
    private static final String HITS_RATE_15M = "recent_hits_per_sec";
    private final static String CAPACITY = "size_max";
    private final static String ENTRIES = "entries";

    CachesTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "caches")
                           .comment("View efficiency of system caches")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn(NAME, UTF8Type.instance)
                           .addRegularColumn(CAPACITY, LongType.instance)
                           .addRegularColumn(SIZE_USED, LongType.instance)
                           .addRegularColumn(HIT_RATIO, DoubleType.instance)
                           .addRegularColumn(HITS, LongType.instance)
                           .addRegularColumn(HITS_RATE_15M, LongType.instance)
                           .addRegularColumn(REQUESTS, LongType.instance)
                           .addRegularColumn(REQUESTS_RATE_15M, LongType.instance)
                           .addRegularColumn(ENTRIES, Int32Type.instance)
                           .build());
    }

    private void addResult(SimpleDataSet result, String name, CacheMetrics metrics)
    {
        result.row(name)
            .column(CAPACITY, metrics.capacity.getValue())
            .column(SIZE_USED, metrics.size.getValue())
            .column(HIT_RATIO, metrics.hitRate.getValue())
            .column(HITS, metrics.hits.getCount())
            .column(HITS_RATE_15M, (long) metrics.hits.getFifteenMinuteRate())
            .column(REQUESTS, metrics.requests.getCount())
            .column(REQUESTS_RATE_15M, (long) metrics.requests.getFifteenMinuteRate())
            .column(ENTRIES, metrics.entries.getValue());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        addResult(result, "key", CacheService.instance.keyCache.getMetrics());
        addResult(result, "row", CacheService.instance.rowCache.getMetrics());
        addResult(result, "counter", CacheService.instance.counterCache.getMetrics());
        return result;
    }
}
