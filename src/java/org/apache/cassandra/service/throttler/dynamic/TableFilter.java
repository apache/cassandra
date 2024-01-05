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

package org.apache.cassandra.service.throttler.dynamic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A filter that efficiently matches a table name against a regex pattern by pre-computing a hashSet based on the
 * regex pattern. The hashSet is expected to be refreshed by TableFiltersRefresher when the keyspace list changes.
 */
public class TableFilter
{
    private static final Logger logger = LoggerFactory.getLogger(TableFilter.class);

    public Pattern regexPattern;
    private Set<String> set;
    private String filterName;

    public TableFilter(TableFiltersRefresher refresher, String filterName)
    {
        this.regexPattern = Pattern.compile(StringUtils.EMPTY);
        this.set = new HashSet<>();
        this.filterName = filterName;
        refresher.addFilter(this);
    }

    public boolean matches(String keyspace, String table)
    {
        return set.contains(String.format("%s.%s", keyspace, table));
    }

    public void refresh()
    {
        refresh(TableFiltersRefresher.getAllKeyspaceDotTables());
    }

    public void refresh(Set<String> keyspaceDotTables)
    {
        long startTime = System.currentTimeMillis();

        HashSet<String> set = new HashSet<>();
        Pattern pattern = regexPattern;
        for (String keyspaceDotTable: keyspaceDotTables)
        {
            assert keyspaceDotTable.contains(".");
            if (pattern.matcher(keyspaceDotTable).matches())
            {
                set.add(keyspaceDotTable);
            }
        }
        this.set = set;

        long endTime = System.currentTimeMillis();
        logger.info("Refreshed table filter in {} ms, filter detail: {}", endTime - startTime, this);
    }

    public void setRegexPatternAndRefresh(String filterRegex)
    {
        regexPattern = Pattern.compile(filterRegex);
        refresh();
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("filter name = ");
        sb.append(filterName);
        sb.append(", regex = ");
        sb.append(regexPattern.pattern());
        sb.append(", list derived from the regex = [ ");
        for (String s : set)
        {
            sb.append(s);
            sb.append(" ");
        }
        sb.append("]");
        return sb.toString();
    }
}
