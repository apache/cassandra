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

package org.apache.cassandra.audit.es;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class EsUtil {

    /**
     * 第一个map有更新，更新到第二个map
     * 如果找不到值的情况，直接返回第二个map
     *
     * @param sqlUpdateMap
     * @param oldMap
     * @return
     */
    public static Map<String, Object> mergeTwoMap(Map<String, Object> sqlUpdateMap, Map<String, Object> oldMap) {
        for (String key1 : oldMap.keySet()) {
            if (sqlUpdateMap.containsKey(key1)) {
                String sqlUpdateMapValue = sqlUpdateMap.get(key1).toString();
                if (!StringUtils.isBlank(sqlUpdateMapValue)) {
                    oldMap.put(key1, sqlUpdateMapValue);
                }
            }
        }

        return oldMap;
    }


    public static String allTrim(String str) {
        String temp = str.trim();
        int index = temp.indexOf(" ");
        while (index > 0) {
            temp = temp.substring(0, index) + temp.substring(index + 1);
            index = temp.indexOf(" ");
        }
        return temp;
    }


    public static String getDslQueryJson(Map<String, Object> maps) {
        StringBuilder sb = new StringBuilder();
        String requesJson = "";
        if (maps.isEmpty()) {
            requesJson = "";
        } else {
            if (maps.size() == 1) {
                sb.append("{\n  \"query\": {\n    \"match\": {\n      \"");
                for (String key1 : maps.keySet()) {
                    String value = maps.get(key1).toString();
                    sb.append(key1 + ".keyword\": \"" + value + "\"");
                }
                sb.append("\n    }\n  }\n}");
            } else if (maps.size() > 1) {
                sb.append("{\n" +
                        "  \"query\": {\n" +
                        "    \"bool\": {\n" +
                        "      \"must\": [");
                int i = 0;
                for (String key : maps.keySet()) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    String value = maps.get(key).toString();
                    sb.append("{\n" +
                            "          \"match_phrase\": {\n" +
                            "            \"" + key + ".keyword\": \"" + value + "\"\n" +
                            "          }\n" +
                            "        }");
                    i++;
                }
                sb.append("      ]\n" +
                        "    }\n" +
                        "  }\n" +
                        "}");
            }

            requesJson = sb.toString().trim();
        }
        return allTrim(requesJson);
    }



    public static String getBulkCreateApiJson(Map<String,Object> maps){
        StringBuilder sb = new StringBuilder();
        sb.append("{\"index\":{\"_id\" : \""+maps.hashCode()+"\" }}\n");
        sb.append("{");
        int i=0;
        for (String key:maps.keySet()){
            String value=maps.get(key).toString();
            if (i > 0){
                sb.append(",");
            }
            sb.append("\""+key+"\":\""+value+"\"");
            i++;
        }
        sb.append("}");
        return sb+"\n";
    }


    public static String getBulkUpdateApiJson(Map<String,Object> maps,String docId){
        StringBuilder sb = new StringBuilder();
        sb.append("{\"update\":{\"_id\":\""+docId+"\"}}\n");
        sb.append("{\"doc\":{");
        int i=0;
        for (String key:maps.keySet()){
            String value=maps.get(key).toString();
            if (i > 0){
                sb.append(",");
            }
            sb.append("\""+key+"\":\""+value+"\"");
            i++;
        }
        sb.append("}}");
        return sb+"\n";
    }


    public static Map<String, Object> getUpdateSqlWhere(String sql) {
        String dbRecord = sql.replace("\"", " ").replace(";", "");
        String[] insertArr = dbRecord.split("where");
        List<String> stringList = Arrays.asList(insertArr);
        if (stringList.size() < 2) {
            System.out.println("update sql 异常");
        }
        String trimSql = stringList.get(1).trim();
        Map<String, Object> maps = new HashMap<>();
        if (trimSql.contains("and")) {
            String[] ands = trimSql.split("and");
            for (int i = 0; i < ands.length; i++) {
                String[] split = ands[i].split("=");
                maps.put(split[0].replace("\'", " ").replace("\"", " "), split[1].replace("\'", " ").replace("\"", " "));
            }

        } else {
            String[] split = trimSql.split("=");
            maps.put(split[0].replace("\'", " ").replace("\"", " "), split[1].replace("\'", " ").replace("\"", " "));
        }

        return maps;
    }

    public static <T> List<T> castList(Object obj, Class<T> clazz) {
        List<T> result = new ArrayList<T>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                result.add(clazz.cast(o));
            }
            return result;
        }
        return null;
    }


    public static boolean isSyncKeyspace(String keyspaceConfiguration, String keyspace) {
        if (StringUtils.isBlank(keyspaceConfiguration)) {
            return true;
        }
        String[] split = keyspaceConfiguration.split(",");
        for (int i = 0; i < split.length; i++) {
            if (split[i].contains("*")) {
                String replace = split[i].replace("*", " ").trim();
                if (keyspace.startsWith(replace)) {
                    return true;
                }
            } else {
                if (split[i].equals(keyspace)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isSyncTable(String tablesConfiguration, String tableName) {
        if (StringUtils.isBlank(tablesConfiguration)) {
            return true;
        }
        String[] split = tablesConfiguration.split(",");
        for (int i = 0; i < split.length; i++) {
            if (split[i].contains("*")) {
                String replace = split[i].replace("*", " ").trim();
                if (tableName.startsWith(replace)) {
                    return true;
                }
            } else {
                if (split[i].equals(tableName)) {
                    return true;
                }
            }
        }
        return false;
    }


}
