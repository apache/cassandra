package org.apache.cassandra.db;

import org.testng.annotations.Test;

import java.io.IOException;

import org.apache.cassandra.service.StorageService;

public class TableTest {
    @Test
    public void testOpen() throws Throwable {
        Table table = Table.open("Mailbox");
        Row row = table.get("35300190:1");
    }
}
