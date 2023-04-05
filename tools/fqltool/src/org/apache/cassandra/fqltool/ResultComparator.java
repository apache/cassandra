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

package org.apache.cassandra.fqltool;


import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultComparator
{
    private static final Logger logger = LoggerFactory.getLogger(ResultComparator.class);
    private final MismatchListener mismatchListener;

    public ResultComparator()
    {
        this(null);
    }

    public ResultComparator(MismatchListener mismatchListener)
    {
        this.mismatchListener = mismatchListener;
    }
    /**
     * Compares the rows in rows
     * the row at position x in rows will have come from host at position x in targetHosts
     */
    public boolean compareRows(List<String> targetHosts, FQLQuery query, List<ResultHandler.ComparableRow> rows)
    {
        if (rows.size() < 2 || rows.stream().allMatch(Objects::isNull))
            return true;

        if (rows.stream().anyMatch(Objects::isNull))
        {
            handleMismatch(targetHosts, query, rows);
            return false;
        }

        ResultHandler.ComparableRow ref = rows.get(0);
        boolean equal = true;
        for (int i = 1; i < rows.size(); i++)
        {
            ResultHandler.ComparableRow compare = rows.get(i);
            if (!ref.equals(compare))
                equal = false;
        }
        if (!equal)
            handleMismatch(targetHosts, query, rows);
        return equal;
    }

    /**
     * Compares the column definitions
     *
     * the column definitions at position x in cds will have come from host at position x in targetHosts
     */
    public boolean compareColumnDefinitions(List<String> targetHosts, FQLQuery query, List<ResultHandler.ComparableColumnDefinitions> cds)
    {
        if (cds.size() < 2)
            return true;

        boolean equal = true;
        List<ResultHandler.ComparableDefinition> refDefs = cds.get(0).asList();
        for (int i = 1; i < cds.size(); i++)
        {
            List<ResultHandler.ComparableDefinition> toCompare = cds.get(i).asList();
            if (!refDefs.equals(toCompare))
                equal = false;
        }
        if (!equal)
            handleColumnDefMismatch(targetHosts, query, cds);
        return equal;
    }

    private void handleMismatch(List<String> targetHosts, FQLQuery query, List<ResultHandler.ComparableRow> rows)
    {
        UUID mismatchUUID = UUID.randomUUID();
        StringBuilder sb = new StringBuilder("{} - MISMATCH Query = {} ");
        for (int i = 0; i < targetHosts.size(); i++)
            sb.append("mismatch").append(i)
              .append('=')
              .append('"').append(targetHosts.get(i)).append(':').append(rows.get(i)).append('"')
              .append(',');

        logger.warn(sb.toString(), mismatchUUID, query);
        try
        {
            if (mismatchListener != null)
                mismatchListener.mismatch(mismatchUUID, targetHosts, query, rows);
        }
        catch (Throwable t)
        {
            logger.error("ERROR notifying listener", t);
        }
    }

    private void handleColumnDefMismatch(List<String> targetHosts, FQLQuery query, List<ResultHandler.ComparableColumnDefinitions> cds)
    {
        UUID mismatchUUID = UUID.randomUUID();
        StringBuilder sb = new StringBuilder("{} - COLUMN DEFINITION MISMATCH Query = {} ");
        for (int i = 0; i < targetHosts.size(); i++)
            sb.append("mismatch").append(i)
              .append('=')
              .append('"').append(targetHosts.get(i)).append(':').append(columnDefinitionsString(cds.get(i))).append('"')
              .append(',');

        logger.warn(sb.toString(), mismatchUUID, query);
        try
        {
            if (mismatchListener != null)
                mismatchListener.columnDefMismatch(mismatchUUID, targetHosts, query, cds);
        }
        catch (Throwable t)
        {
            logger.error("ERROR notifying listener", t);
        }
    }

    private String columnDefinitionsString(ResultHandler.ComparableColumnDefinitions cd)
    {
        StringBuilder sb = new StringBuilder();
        if (cd == null)
            sb.append("NULL");
        else if (cd.wasFailed())
            sb.append("FAILED");
        else
        {
            for (ResultHandler.ComparableDefinition def : cd)
            {
                sb.append(def.toString());
            }
        }
        return sb.toString();
    }
}
