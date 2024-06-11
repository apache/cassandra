package org.apache.cassandra.optimizer;

import static org.junit.Assert.*;
import org.junit.Test;

public class MetadataTest {

    @Test
    public void testMetadata() {
        Metadata metadata = new Metadata();
        metadata.setTableName("users");
        metadata.setIndexName("user_id_idx");
        metadata.setColumns(new String[]{"user_id", "name"});
        metadata.setConditions("user_id = 123");

        assertEquals("users", metadata.getTableName());
        assertEquals("user_id_idx", metadata.getIndexName());
        assertArrayEquals(new String[]{"user_id", "name"}, metadata.getColumns());
        assertEquals("user_id = 123", metadata.getConditions());
    }
}
