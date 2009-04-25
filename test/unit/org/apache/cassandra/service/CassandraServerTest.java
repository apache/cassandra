package org.apache.cassandra.service;

import org.apache.cassandra.ServerTest;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

public class CassandraServerTest extends ServerTest {
    /*
    @Test
    public void test_get_column() throws Throwable {
        CassandraServer server = new CassandraServer();
        server.start();

        try {
            column_t c1 = new column_t("c1", "0", 0L);
            column_t c2 = new column_t("c2", "0", 0L);
            List<column_t> columns = new ArrayList<column_t>();
            columns.add(c1);
            columns.add(c2);
            Map<String, List<column_t>> cfmap = new HashMap<String, List<column_t>>();
            cfmap.put("Standard1", columns);
            cfmap.put("Standard2", columns);

            batch_mutation_t m = new batch_mutation_t("Table1", "key1", cfmap);
            server.batch_insert(m, 1);

            column_t column;
            column = server.get_column("Table1", "key1", "Standard1:c2");
            assert column.value.equals("0");

            column = server.get_column("Table1", "key1", "Standard2:c2");
            assert column.value.equals("0");

            ArrayList<column_t> column_ts = server.get_slice_strong("Table1", "key1", "Standard1", -1, -1);
            assert column_ts.size() == 2;
        } finally {
            server.shutdown();
        }
    }
    */
}
