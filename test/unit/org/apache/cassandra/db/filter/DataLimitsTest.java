/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.filter.DataLimits.NO_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class DataLimitsTest
{
    private final static Logger logger = LoggerFactory.getLogger(DataLimitsTest.class);

    ByteBuffer lastReturnedKey = ByteBufferUtil.bytes("lastReturnedKey");

    DataLimits cqlLimits = DataLimits.cqlLimits(19, 17);
    DataLimits cqlLimitsForPagingInRows = cqlLimits.forPaging(PageSize.inRows(13));
    DataLimits cqlLimitsForPagingInBytes = cqlLimits.forPaging(PageSize.inBytes(13));
    DataLimits cqlLimitsForPagingInRowsWithLastRow = cqlLimits.forPaging(PageSize.inRows(13), lastReturnedKey, 5);
    DataLimits cqlLimitsForPagingInBytesWithLastRow = cqlLimits.forPaging(PageSize.inBytes(13), lastReturnedKey, 5);
    DataLimits groupByLimits = DataLimits.groupByLimits(19, 17, NO_LIMIT, NO_LIMIT, AggregationSpecification.AGGREGATE_EVERYTHING);
    DataLimits groupByLimitsForPagingInRows = groupByLimits.forPaging(PageSize.inRows(13));
    DataLimits groupByLimitsForPagingInBytes = groupByLimits.forPaging(PageSize.inBytes(13));
    DataLimits groupByLimitsForPagingInRowsWithLastRow = groupByLimits.forPaging(PageSize.inRows(13), lastReturnedKey, 5);
    DataLimits groupByLimitsForPagingInBytesWithLastRow = groupByLimits.forPaging(PageSize.inBytes(13), lastReturnedKey, 5);

    @BeforeClass
    public static void initClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void serializationTest()
    {
        for (MessagingService.Version version : MessagingService.Version.values())
        {
            checkSerialization(version, cqlLimits, "cql limits");
            checkSerialization(version, cqlLimitsForPagingInRows, "cql limits for paging in rows");
            checkSerialization(version, cqlLimitsForPagingInBytes, "cql limits for paging in bytes");
            checkSerialization(version, cqlLimitsForPagingInRowsWithLastRow, "cql limits for paging in rows with last row");
            checkSerialization(version, cqlLimitsForPagingInBytesWithLastRow, "cql limits for paging in bytes with last row");
            checkSerialization(version, groupByLimits, "group by limits");
            checkSerialization(version, groupByLimitsForPagingInRows, "group by limits for paging in rows");
            checkSerialization(version, groupByLimitsForPagingInBytes, "group by limits for paging in bytes");
            checkSerialization(version, groupByLimitsForPagingInRowsWithLastRow, "group by limits for paging in rows with last row");
            checkSerialization(version, groupByLimitsForPagingInBytesWithLastRow, "group by limits for paging in bytes with last row");
        }
    }

    @Test
    public void toStringTest()
    {
        String lastRetKeyStr = String.format("lastReturnedKey=%s", ByteBufferUtil.bytesToHex(lastReturnedKey));
        String lastRetKeyRemainingStr = "lastReturnedKeyRemaining=5";

        assertThat(cqlLimits.toString()).contains("ROWS LIMIT 19").contains("PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT");
        assertThat(cqlLimitsForPagingInRows.toString()).contains("ROWS LIMIT 13").contains("PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT");
        assertThat(cqlLimitsForPagingInBytes.toString()).contains("BYTES LIMIT 13").contains("ROWS LIMIT 19").contains("PER PARTITION LIMIT 17");
        assertThat(cqlLimitsForPagingInRowsWithLastRow.toString()).contains("ROWS LIMIT 13").contains("PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);
        assertThat(cqlLimitsForPagingInBytesWithLastRow.toString()).contains("BYTES LIMIT 13").contains("ROWS LIMIT 19").contains("PER PARTITION LIMIT 17").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);

        assertThat(groupByLimits.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").doesNotContain("ROWS LIMIT").doesNotContain("BYTES LIMIT");
        assertThat(groupByLimitsForPagingInRows.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").contains("ROWS LIMIT 13").doesNotContain("BYTES LIMIT");
        assertThat(groupByLimitsForPagingInBytes.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").doesNotContain("ROWS LIMIT").contains("BYTES LIMIT 13");
        assertThat(groupByLimitsForPagingInRowsWithLastRow.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").contains("ROWS LIMIT 13").doesNotContain("BYTES LIMIT").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);
        assertThat(groupByLimitsForPagingInBytesWithLastRow.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").doesNotContain("ROWS LIMIT").contains("BYTES LIMIT 13").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);
    }

    private void checkSerialization(MessagingService.Version version, DataLimits limits, String name)
    {
        String msg = String.format("serialization of %s for version %s", name, version);
        int size = (int) DataLimits.serializer.serializedSize(limits, version.value, null);
        try (DataOutputBuffer out = new DataOutputBuffer(2 * size))
        {
            DataLimits.serializer.serialize(limits, out, version.value, null);
            out.flush();
            assertThat(out.getLength()).describedAs(msg).isEqualTo(size);
            try (DataInputBuffer in = new DataInputBuffer(out.getData()))
            {
                DataLimits deserializedLimits = DataLimits.serializer.deserialize(in, version.value, null);
                assertThat(deserializedLimits.count()).describedAs(msg).isEqualTo(limits.count());

                if (version.value >= MessagingService.VERSION_41)
                    assertThat(deserializedLimits.bytes()).describedAs(msg).isEqualTo(limits.bytes());
                else
                    assertThat(deserializedLimits.bytes()).describedAs(msg).isEqualTo(NO_LIMIT);

                assertThat(deserializedLimits.rows()).describedAs(msg).isEqualTo(limits.rows());
                assertThat(deserializedLimits.perPartitionCount()).describedAs(msg).isEqualTo(limits.perPartitionCount());
                assertThat(deserializedLimits.isDistinct()).describedAs(msg).isEqualTo(limits.isDistinct());
                assertThat(deserializedLimits.isUnlimited()).describedAs(msg).isEqualTo(limits.isUnlimited());
                assertThat(deserializedLimits.kind()).describedAs(msg).isEqualTo(limits.kind());
                assertThat(deserializedLimits.isGroupByLimit()).describedAs(msg).isEqualTo(limits.isGroupByLimit());
            }
            catch (IOException e)
            {
                logger.error("Failed to deserialize: " + msg, e);
                fail(msg);
            }
        }
        catch (IOException e)
        {
            logger.error("Failed to serialize: " + msg, e);
            fail(msg);
        }
    }
}