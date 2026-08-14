/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.functional;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class SaiDiskSizeTest extends SAITester
{
    private static final Logger logger = LoggerFactory.getLogger(SaiDiskSizeTest.class);

    // Fixed base timestamp for deterministic test data (2024-01-01 00:00:00 UTC)
    private static final long BASE_TIMESTAMP_MILLIS = 1704067200000L;

    @Parameterized.Parameter
    public Version saiFormat;

    @Parameterized.Parameter(1)
    public int expectedDiskSizeFor2SSTables;

    @Parameterized.Parameter(2)
    public int expectedDiskSizeForCompactedSSTable;

    @Parameterized.Parameter(3)
    public String pkColumns;

    @Parameterized.Parameter(4)
    public int rowsPerPartition;

    /**
     * The expected sizes were determined empirically to satisfy the result of both flush and compaction.
     * To understand the difference check {@link Version} and on disk components.
     * There are no vectors involved, thus the expected sizes are not affected by changes to Vector format.
     *
     * @return a collection of parameters to test
     */
    @Parameterized.Parameters(name = "saiFormat={0}, rowsPerPartition={4}")
    public static Collection<Object[]> generateParameters()
    {
        return Version.ALL.stream()
                          .flatMap(v -> {
                              switch (v.toString())
                              {
                                  case "aa":
                                      return Stream.of(
                                      new Object[]{ v, 24989, 24989, "pk", 1 },
                                      new Object[]{ v, 26026, 26181, "pk, v_int", 2 },
                                      new Object[]{ v, 28526, 26603, "pk, v_int", 100 });
                                  case "ba":
                                  case "ca":
                                  case "db":
                                  case "dc":
                                      return Stream.of(
                                      new Object[]{ v, 131489, 131312, "pk", 1 },
                                      new Object[]{ v, 115177, 115009, "pk, v_int", 2 },
                                      new Object[]{ v, 55861, 55720, "pk, v_int", 100 });
                                  case "eb":
                                  case "ec":
                                      return Stream.of(
                                      new Object[]{ v, 134761, 132841, "pk", 1 },
                                      new Object[]{ v, 118449, 116538, "pk, v_int", 2 },
                                      new Object[]{ v, 59133, 57249, "pk, v_int", 100 });
                                  case "ed":
                                  case "fa":
                                  case "fb":
                                      return Stream.of(
                                      new Object[]{ v, 134777, 132849, "pk", 1 },
                                      new Object[]{ v, 118465, 116546, "pk, v_int", 2 },
                                      new Object[]{ v, 59149, 57257, "pk, v_int", 100 });
                                  case "ga":
                                  default:
                                      return // A new version assumes the latest size by default
                                      Stream.of(
                                      new Object[]{ v, 34901, 34689, "pk", 1 },
                                      new Object[]{ v, 39313, 39313, "pk, v_int", 2 },
                                      new Object[]{ v, 33549, 31315, "pk, v_int", 100 });
                              }
                          })
                          .collect(Collectors.toList());
    }

    @Before
    public void setVersion()
    {
        SAIUtil.setCurrentVersion(saiFormat);
    }

    @Test
    public void testIndexDiskSizeAcrossVersions() throws UnknownHostException
    {
        createTable("CREATE TABLE %s (" +
                    "pk int, " +
                    "v_int int, " +
                    "v_ascii ascii, " +
                    "v_bigint bigint, " +
                    "v_blob blob, " +
                    "v_boolean boolean, " +
                    "v_decimal decimal, " +
                    "v_double double, " +
                    "v_float float, " +
                    "v_text text, " +
                    "v_timestamp timestamp, " +
                    "v_uuid uuid, " +
                    "v_varchar varchar, " +
                    "v_varint varint, " +
                    "v_timeuuid timeuuid, " +
                    "v_inet inet, " +
                    "v_date date, " +
                    "v_time time, " +
                    "v_smallint smallint, " +
                    "v_tinyint tinyint, " +
                    "v_duration duration, " +
                    "PRIMARY KEY (" + pkColumns + "))");

        verifyNoIndexFiles();
        createIndex("CREATE CUSTOM INDEX ON %s(v_int) USING 'StorageAttachedIndex'");

        waitForTableIndexesQueryable();

        // Split generateParameters into 2 sstable segments
        insertRowsIntoOneSegment(1000, 0);
        flush();
        insertRowsIntoOneSegment(1000, 1000);
        flush();

        long diskSize = indexDiskSpaceUse();
        logger.info("Disk size for SAI version {}: {}", saiFormat, diskSize);
        assertThat(diskSize)
        .as("Disk size for SAI version %s before compaction", saiFormat)
        .isLessThanOrEqualTo(expectedDiskSizeFor2SSTables)
        .isGreaterThan((long) (expectedDiskSizeFor2SSTables * 0.92));

        compact();

        diskSize = indexDiskSpaceUse();
        logger.info("Disk size for SAI version {}: {}", saiFormat, diskSize);
        assertThat(diskSize)
        .as("Disk size for SAI version %s after compaction", saiFormat)
        .isLessThanOrEqualTo(expectedDiskSizeForCompactedSSTable)
        .isGreaterThan((long) (expectedDiskSizeForCompactedSSTable * 0.92));
    }

    private void insertRowsIntoOneSegment(int nrRows, int startRow) throws UnknownHostException
    {
        assert nrRows % rowsPerPartition == 0;
        int nrOfPartitions = nrRows / rowsPerPartition;
        assert nrOfPartitions > 0;
        for (int i = startRow; i < startRow + nrRows; i++)
        {
            execute("INSERT INTO %s (pk, v_int, v_ascii, v_bigint, v_blob, v_boolean, " +
                    "v_decimal, v_double, v_float, v_text, v_timestamp, v_uuid, v_varchar, " +
                    "v_varint, v_timeuuid, v_inet, v_date, v_time, v_smallint, v_tinyint, v_duration) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    startRow + i % nrOfPartitions, // StartRow allows starting new partitions for new segment
                    i,
                    "ascii_" + i,
                    (long) i * 1000000,
                    ByteBuffer.wrap(("blob_" + i).getBytes()),
                    i % 2 == 0,
                    new BigDecimal(i + ".123"),
                    i * 1.5,
                    (float) (i * 2.5),
                    "text_value_" + i,
                    new Date(BASE_TIMESTAMP_MILLIS + i * 1000L),
                    UUID.randomUUID(),
                    "varchar_" + i,
                    BigInteger.valueOf(i).multiply(BigInteger.valueOf(100)),
                    UUID.fromString("00000000-0000-1000-8000-" +
                                    String.format("%012d", i)),
                    InetAddressAndPort.getByName("127.0.0." + (i % 256)).address,
                    i + 1,
                    (long) i * 1000000000L,
                    (short) (i % 32767),
                    (byte) (i % 128),
                    org.apache.cassandra.cql3.Duration.newInstance(i % 12, i % 30, i * 1000000000L)
            );
        }
    }
}
