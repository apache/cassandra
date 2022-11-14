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

package org.apache.cassandra.cql3;

import org.junit.Test;

import org.apache.cassandra.cql3.PageSize.PageUnit;
import org.apache.cassandra.db.filter.DataLimits;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;


public class PageSizeTest
{

    @Test
    public void inRows()
    {
        PageSize ps = PageSize.inRows(10);
        assertThat(ps.getSize()).isEqualTo(10);
        assertThat(ps.getUnit()).isEqualTo(PageUnit.ROWS);
        assertThat(ps.isDefined()).isTrue();
        assertThat(ps.rows()).isEqualTo(10);
        assertThat(ps.bytes()).isEqualTo(DataLimits.NO_LIMIT);

        assertThat(ps.minRowsCount(20)).isEqualTo(10);
        assertThat(ps.minRowsCount(5)).isEqualTo(5);
        assertThat(ps.minBytesCount(20)).isEqualTo(20);
        assertThat(ps.minBytesCount(5)).isEqualTo(5);

        assertThat(ps.withDecreasedRows(3)).isEqualTo(PageSize.inRows(7));
        assertThat(ps.withDecreasedRows(13)).isEqualTo(PageSize.inRows(0));
        assertThat(ps.withDecreasedRows(-3)).isEqualTo(PageSize.inRows(13));
        assertThat(ps.withDecreasedRows(Integer.MIN_VALUE)).isEqualTo(PageSize.inRows(DataLimits.NO_LIMIT));

        assertThat(ps.withDecreasedBytes(3)).isEqualTo(ps);
        assertThat(ps.withDecreasedBytes(-3)).isEqualTo(ps);

        assertThat(ps.isCompleted(9, PageUnit.ROWS)).isFalse();
        assertThat(ps.isCompleted(10, PageUnit.ROWS)).isTrue();
        assertThat(ps.isCompleted(9, PageUnit.BYTES)).isFalse();
        assertThat(ps.isCompleted(10, PageUnit.BYTES)).isFalse();

        assertThat(ps.toString()).contains("10 rows");

        assertThatIllegalArgumentException().isThrownBy(() -> PageSize.inRows(-1));
    }

    @Test
    public void inBytes()
    {
        PageSize ps = PageSize.inBytes(10);
        assertThat(ps.getSize()).isEqualTo(10);
        assertThat(ps.getUnit()).isEqualTo(PageUnit.BYTES);
        assertThat(ps.isDefined()).isTrue();
        assertThat(ps.rows()).isEqualTo(DataLimits.NO_LIMIT);
        assertThat(ps.bytes()).isEqualTo(10);

        assertThat(ps.minRowsCount(20)).isEqualTo(20);
        assertThat(ps.minRowsCount(5)).isEqualTo(5);
        assertThat(ps.minBytesCount(20)).isEqualTo(10);
        assertThat(ps.minBytesCount(5)).isEqualTo(5);

        assertThat(ps.withDecreasedBytes(3)).isEqualTo(PageSize.inBytes(7));
        assertThat(ps.withDecreasedBytes(13)).isEqualTo(PageSize.inBytes(0));
        assertThat(ps.withDecreasedBytes(-3)).isEqualTo(PageSize.inBytes(13));
        assertThat(ps.withDecreasedBytes(Integer.MIN_VALUE)).isEqualTo(PageSize.inBytes(DataLimits.NO_LIMIT));

        assertThat(ps.withDecreasedRows(3)).isEqualTo(ps);
        assertThat(ps.withDecreasedRows(-3)).isEqualTo(ps);

        assertThat(ps.isCompleted(9, PageUnit.BYTES)).isFalse();
        assertThat(ps.isCompleted(10, PageUnit.BYTES)).isTrue();
        assertThat(ps.isCompleted(9, PageUnit.ROWS)).isFalse();
        assertThat(ps.isCompleted(10, PageUnit.ROWS)).isFalse();

        assertThat(ps.toString()).contains("10 bytes");

        assertThatIllegalArgumentException().isThrownBy(() -> PageSize.inBytes(-1));
    }

    @Test
    public void testNone()
    {
        assertThat(PageSize.NONE.toString()).contains("unlimited");
        assertThat(PageSize.NONE.bytes()).isEqualTo(DataLimits.NO_LIMIT);
        assertThat(PageSize.NONE.rows()).isEqualTo(DataLimits.NO_LIMIT);
    }
}