package org.apache.cassandra.service;

import org.apache.cassandra.ServerTest;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

import com.facebook.thrift.TException;

public class CassandraServerTest extends ServerTest {
    /*
    TODO fix resetting server so this works
    @Test
    public void test_get_range_empty() throws IOException, TException {
        CassandraServer server = new CassandraServer();
        server.start();

        assert CollectionUtils.EMPTY_COLLECTION.equals(server.get_range(DatabaseDescriptor.getTableName(), ""));
    }
    */

    /*
    @Test
    public void test_get_range() throws IOException, TException, CassandraException
    {
        CassandraServer server = new CassandraServer();
        try
        {
            server.start();
        }
        catch (Throwable throwable)
        {
            throw new RuntimeException(throwable);
        }

        // TODO insert some data
        try {
            String last = null;
            for (String key : server.get_range(DatabaseDescriptor.getTableName(), "key1")) {
                if (last != null) {
                    assert last.compareTo(key) < 0;
                }
                last = key;
            }
        } finally {
            server.shutdown();
        }
    }
    */

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
