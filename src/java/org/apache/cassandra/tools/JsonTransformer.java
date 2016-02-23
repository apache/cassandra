package org.apache.cassandra.tools;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.impl.Indenter;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.codehaus.jackson.util.DefaultPrettyPrinter.NopIndenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JsonTransformer
{

    private static final Logger logger = LoggerFactory.getLogger(JsonTransformer.class);

    private static final JsonFactory jsonFactory = new JsonFactory();

    private final JsonGenerator json;

    private final CompactIndenter objectIndenter = new CompactIndenter();

    private final CompactIndenter arrayIndenter = new CompactIndenter();

    private final CFMetaData metadata;

    private final ISSTableScanner currentScanner;

    private long currentPosition = 0;

    private JsonTransformer(JsonGenerator json, ISSTableScanner currentScanner, CFMetaData metadata)
    {
        this.json = json;
        this.metadata = metadata;
        this.currentScanner = currentScanner;

        DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
        prettyPrinter.indentObjectsWith(objectIndenter);
        prettyPrinter.indentArraysWith(arrayIndenter);
        json.setPrettyPrinter(prettyPrinter);
    }

    public static void toJson(ISSTableScanner currentScanner, Stream<UnfilteredRowIterator> partitions, CFMetaData metadata, OutputStream out)
            throws IOException
    {
        try (JsonGenerator json = jsonFactory.createJsonGenerator(new OutputStreamWriter(out, "UTF-8")))
        {
            JsonTransformer transformer = new JsonTransformer(json, currentScanner, metadata);
            json.writeStartArray();
            partitions.forEach(transformer::serializePartition);
            json.writeEndArray();
        }
    }

    public static void keysToJson(ISSTableScanner currentScanner, Stream<DecoratedKey> keys, CFMetaData metadata, OutputStream out) throws IOException
    {
        try (JsonGenerator json = jsonFactory.createJsonGenerator(new OutputStreamWriter(out, "UTF-8")))
        {
            JsonTransformer transformer = new JsonTransformer(json, currentScanner, metadata);
            json.writeStartArray();
            keys.forEach(transformer::serializePartitionKey);
            json.writeEndArray();
        }
    }

    private void updatePosition()
    {
        this.currentPosition = currentScanner.getCurrentPosition();
    }

    private void serializePartitionKey(DecoratedKey key)
    {
        AbstractType<?> keyValidator = metadata.getKeyValidator();
        objectIndenter.setCompact(true);
        try
        {
            arrayIndenter.setCompact(true);
            json.writeStartArray();
            if (keyValidator instanceof CompositeType)
            {
                // if a composite type, the partition has multiple keys.
                CompositeType compositeType = (CompositeType) keyValidator;
                ByteBuffer keyBytes = key.getKey().duplicate();
                // Skip static data if it exists.
                if (keyBytes.remaining() >= 2)
                {
                    int header = ByteBufferUtil.getShortLength(keyBytes, keyBytes.position());
                    if ((header & 0xFFFF) == 0xFFFF)
                    {
                        ByteBufferUtil.readShortLength(keyBytes);
                    }
                }

                int i = 0;
                while (keyBytes.remaining() > 0 && i < compositeType.getComponents().size())
                {
                    AbstractType<?> colType = compositeType.getComponents().get(i);

                    ByteBuffer value = ByteBufferUtil.readBytesWithShortLength(keyBytes);
                    String colValue = colType.getString(value);

                    json.writeString(colValue);

                    byte b = keyBytes.get();
                    if (b != 0)
                    {
                        break;
                    }
                    ++i;
                }
            }
            else
            {
                // if not a composite type, assume a single column partition key.
                assert metadata.partitionKeyColumns().size() == 1;
                json.writeString(keyValidator.getString(key.getKey()));
            }
            json.writeEndArray();
            objectIndenter.setCompact(false);
            arrayIndenter.setCompact(false);
        }
        catch (IOException e)
        {
            logger.error("Failure serializing partition key.", e);
        }
    }

    private void serializePartition(UnfilteredRowIterator partition)
    {
        String key = metadata.getKeyValidator().getString(partition.partitionKey().getKey());
        try
        {
            json.writeStartObject();

            json.writeFieldName("partition");
            json.writeStartObject();
            json.writeFieldName("key");
            serializePartitionKey(partition.partitionKey());
            json.writeNumberField("position", this.currentScanner.getCurrentPosition());

            if (!partition.partitionLevelDeletion().isLive())
            {
                json.writeFieldName("deletion_info");
                objectIndenter.setCompact(true);
                json.writeStartObject();
                json.writeFieldName("deletion_time");
                json.writeNumber(partition.partitionLevelDeletion().markedForDeleteAt());
                json.writeFieldName("tstamp");
                json.writeNumber(partition.partitionLevelDeletion().localDeletionTime());
                json.writeEndObject();
                objectIndenter.setCompact(false);
                json.writeEndObject();
            }
            else
            {
                json.writeEndObject();
                json.writeFieldName("rows");
                json.writeStartArray();
                updatePosition();
                if (!partition.staticRow().isEmpty())
                {
                    serializeRow(partition.staticRow());
                }
                Unfiltered unfiltered;
                updatePosition();
                while (partition.hasNext())
                {
                    unfiltered = partition.next();
                    if (unfiltered instanceof Row)
                    {
                        serializeRow((Row) unfiltered);
                    }
                    else if (unfiltered instanceof RangeTombstoneMarker)
                    {
                        serializeTombstone((RangeTombstoneMarker) unfiltered);
                    }
                    updatePosition();
                }
                json.writeEndArray();
            }

            json.writeEndObject();
        }
        catch (IOException e)
        {
            logger.error("Fatal error parsing partition: {}", key, e);
        }
    }

    private void serializeRow(Row row)
    {
        try
        {
            json.writeStartObject();
            String rowType = row.isStatic() ? "static_block" : "row";
            json.writeFieldName("type");
            json.writeString(rowType);
            json.writeNumberField("position", this.currentPosition);

            // Only print clustering information for non-static rows.
            if (!row.isStatic())
            {
                serializeClustering(row.clustering());
            }

            LivenessInfo liveInfo = row.primaryKeyLivenessInfo();
            if (!liveInfo.isEmpty())
            {
                objectIndenter.setCompact(false);
                json.writeFieldName("liveness_info");
                objectIndenter.setCompact(true);
                json.writeStartObject();
                json.writeFieldName("tstamp");
                json.writeNumber(liveInfo.timestamp());
                if (liveInfo.isExpiring())
                {
                    json.writeFieldName("ttl");
                    json.writeNumber(liveInfo.ttl());
                    json.writeFieldName("expires_at");
                    json.writeNumber(liveInfo.localExpirationTime());
                    json.writeFieldName("expired");
                    json.writeBoolean(liveInfo.localExpirationTime() < (System.currentTimeMillis() / 1000));
                }
                json.writeEndObject();
                objectIndenter.setCompact(false);
            }

            // If this is a deletion, indicate that, otherwise write cells.
            if (!row.deletion().isLive())
            {
                json.writeFieldName("deletion_info");
                objectIndenter.setCompact(true);
                json.writeStartObject();
                json.writeFieldName("deletion_time");
                json.writeNumber(row.deletion().time().markedForDeleteAt());
                json.writeFieldName("tstamp");
                json.writeNumber(row.deletion().time().localDeletionTime());
                json.writeEndObject();
                objectIndenter.setCompact(false);
            }
            else
            {
                json.writeFieldName("cells");
                json.writeStartArray();
                row.cells().forEach(c -> serializeCell(c, liveInfo));
                json.writeEndArray();
            }
            json.writeEndObject();
        }
        catch (IOException e)
        {
            logger.error("Fatal error parsing row.", e);
        }
    }

    private void serializeTombstone(RangeTombstoneMarker tombstone)
    {
        try
        {
            json.writeStartObject();
            json.writeFieldName("type");

            if (tombstone instanceof RangeTombstoneBoundMarker)
            {
                json.writeString("range_tombstone_bound");
                RangeTombstoneBoundMarker bm = (RangeTombstoneBoundMarker) tombstone;
                serializeBound(bm.clustering(), bm.deletionTime());
            }
            else
            {
                assert tombstone instanceof RangeTombstoneBoundaryMarker;
                json.writeString("range_tombstone_boundary");
                RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker) tombstone;
                serializeBound(bm.openBound(false), bm.openDeletionTime(false));
                serializeBound(bm.closeBound(false), bm.closeDeletionTime(false));
            }
            json.writeEndObject();
            objectIndenter.setCompact(false);
        }
        catch (IOException e)
        {
            logger.error("Failure parsing tombstone.", e);
        }
    }

    private void serializeBound(RangeTombstone.Bound bound, DeletionTime deletionTime) throws IOException
    {
        json.writeFieldName(bound.isStart() ? "start" : "end");
        json.writeStartObject();
        json.writeFieldName("type");
        json.writeString(bound.isInclusive() ? "inclusive" : "exclusive");
        serializeClustering(bound.clustering());
        serializeDeletion(deletionTime);
        json.writeEndObject();
    }

    private void serializeClustering(ClusteringPrefix clustering) throws IOException
    {
        if (clustering.size() > 0)
        {
            json.writeFieldName("clustering");
            objectIndenter.setCompact(true);
            json.writeStartArray();
            arrayIndenter.setCompact(true);
            List<ColumnDefinition> clusteringColumns = metadata.clusteringColumns();
            for (int i = 0; i < clusteringColumns.size(); i++)
            {
                ColumnDefinition column = clusteringColumns.get(i);
                if (i >= clustering.size())
                {
                    json.writeString("*");
                }
                else
                {
                    json.writeString(column.cellValueType().getString(clustering.get(i)));
                }
            }
            json.writeEndArray();
            objectIndenter.setCompact(false);
            arrayIndenter.setCompact(false);
        }
    }

    private void serializeDeletion(DeletionTime deletion) throws IOException
    {
        json.writeFieldName("deletion_info");
        objectIndenter.setCompact(true);
        json.writeStartObject();
        json.writeFieldName("deletion_time");
        json.writeNumber(deletion.markedForDeleteAt());
        json.writeFieldName("tstamp");
        json.writeNumber(deletion.localDeletionTime());
        json.writeEndObject();
        objectIndenter.setCompact(false);
    }

    private void serializeCell(Cell cell, LivenessInfo liveInfo)
    {
        try
        {
            json.writeStartObject();
            objectIndenter.setCompact(true);
            json.writeFieldName("name");
            AbstractType<?> type = cell.column().type;
            json.writeString(cell.column().name.toCQLString());

            if (cell.path() != null && cell.path().size() > 0)
            {
                CollectionType ct = (CollectionType) type;
                json.writeFieldName("path");
                arrayIndenter.setCompact(true);
                json.writeStartArray();
                for (int i = 0; i < cell.path().size(); i++)
                {
                    json.writeString(ct.nameComparator().getString(cell.path().get(i)));
                }
                json.writeEndArray();
                arrayIndenter.setCompact(false);
            }
            if (cell.isTombstone())
            {
                json.writeFieldName("deletion_time");
                json.writeNumber(cell.localDeletionTime());
            }
            else
            {
                json.writeFieldName("value");
                json.writeString(cell.column().cellValueType().getString(cell.value()));
            }
            if (liveInfo.isEmpty() || cell.timestamp() != liveInfo.timestamp())
            {
                json.writeFieldName("tstamp");
                json.writeNumber(cell.timestamp());
            }
            if (cell.isExpiring() && (liveInfo.isEmpty() || cell.ttl() != liveInfo.ttl()))
            {
                json.writeFieldName("ttl");
                json.writeNumber(cell.ttl());
                json.writeFieldName("expires_at");
                json.writeNumber(cell.localDeletionTime());
                json.writeFieldName("expired");
                json.writeBoolean(!cell.isLive((int) (System.currentTimeMillis() / 1000)));
            }
            json.writeEndObject();
            objectIndenter.setCompact(false);
        }
        catch (IOException e)
        {
            logger.error("Failure parsing cell.", e);
        }
    }

    /**
     * A specialized {@link Indenter} that enables a 'compact' mode which puts all subsequent json values on the same
     * line. This is manipulated via {@link CompactIndenter#setCompact(boolean)}
     */
    private static final class CompactIndenter extends NopIndenter
    {

        private static final int INDENT_LEVELS = 16;
        private final char[] indents;
        private final int charsPerLevel;
        private final String eol;
        private static final String space = " ";

        private boolean compact = false;

        CompactIndenter()
        {
            this("  ", System.lineSeparator());
        }

        CompactIndenter(String indent, String eol)
        {
            this.eol = eol;

            charsPerLevel = indent.length();

            indents = new char[indent.length() * INDENT_LEVELS];
            int offset = 0;
            for (int i = 0; i < INDENT_LEVELS; i++)
            {
                indent.getChars(0, indent.length(), indents, offset);
                offset += indent.length();
            }
        }

        @Override
        public boolean isInline()
        {
            return false;
        }

        /**
         * Configures whether or not subsequent json values should be on the same line delimited by string or not.
         *
         * @param compact
         *            Whether or not to compact.
         */
        public void setCompact(boolean compact)
        {
            this.compact = compact;
        }

        @Override
        public void writeIndentation(JsonGenerator jg, int level)
        {
            try
            {
                if (!compact)
                {
                    jg.writeRaw(eol);
                    if (level > 0)
                    { // should we err on negative values (as there's some flaw?)
                        level *= charsPerLevel;
                        while (level > indents.length)
                        { // unlike to happen but just in case
                            jg.writeRaw(indents, 0, indents.length);
                            level -= indents.length;
                        }
                        jg.writeRaw(indents, 0, level);
                    }
                }
                else
                {
                    jg.writeRaw(space);
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}