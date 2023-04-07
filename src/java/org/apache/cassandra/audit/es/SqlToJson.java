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

import java.util.*;

public class SqlToJson {


//    public static String sqlInsertToJosn(String sql){
//        String dbRecord = sql+"\n";
//        String[] insertArr = dbRecord.split("INSERT");
//        List<String> stringList = Arrays.asList(insertArr);
//        return EsUtil.allTrim(dbRecordToJsonStr(stringList));
//    }

    public static Map<String,Object> sqlInsertToJosn(String sql) {
        String dbRecordSql = sql+"\n";
        String[] insertArr = dbRecordSql.split("INSERT");
        List<String> dbRecordList = Arrays.asList(insertArr);
        if (null == dbRecordList || dbRecordList.size() == 0) {
            return null;
        }

        Map<String,Object> maps=new HashMap<>();

        for (int i = 0; i < dbRecordList.size(); i++) {

            String dbRecord = dbRecordList.get(i);
            if (!dbRecord.contains("(")) {
                continue;
            }

            String fields = dbRecord.substring(dbRecord.indexOf("(") + 1, dbRecord.indexOf(")"));
            String values = dbRecord.substring(dbRecord.lastIndexOf("(") + 1, dbRecord.lastIndexOf(")"));
            String replacedFields = fields.replace("`", "").trim();
            String replacedValues = values.replace("'", "").trim();
            String[] fieldsArr = replacedFields.split(",");
            String[] valuesArr = replacedValues.split(",");
            for (int j = 0; j < fieldsArr.length; j++) {
                maps.put(fieldsArr[j].trim(),valuesArr[j].trim());
            }
        }
        return maps;
    }

    public static Map sqlUpdateToJson(String sql){
        String dbRecord= sql.replace("\""," ").replace(";","");
        String[] insertArr = dbRecord.split(" ");
        List<String> stringList = Arrays.asList(insertArr);
        Map<String,Object> maps=new HashMap<>();
        stringList.forEach(s -> {
            if (s.contains(",")){
                String[] splitd = s.split(",");
                for (int i = 0; i < splitd.length; i++) {
                    if (splitd[i].contains("=")){
                        String[] split = splitd[i].split("=");
                        maps.put(split[0].replace("\'"," ").replace("\""," "),split[1].replace("\'"," ").replace("\""," "));
                    }
                }
            }else {
                if (s.contains("=")) {
                    String[] split = s.split("=");
                    maps.put(split[0].replace("\'", " ").replace("\"", " "), split[1].replace("\'", " ").replace("\"", " "));
                }
            }
        });
        return maps;
    }


    public static Map sqlDeleteToJson(String sql){
        String dbRecord= sql.replace("\""," ").replace(";","").toLowerCase(Locale.ROOT);
        String[] insertArr = dbRecord.split(" ");
        List<String> stringList = Arrays.asList(insertArr);
        Map<String,Object> maps=new HashMap<>();
        stringList.forEach(str->{
            if (str.contains("=")){
                String[] split = str.split("=");
                maps.put(split[0].trim(),split[1].replace("'"," ").trim());
            }
        });
        return maps;
    }

    public static String getFirstId(String sql){
        String dbRecord = sql+"\n";
        String[] insertArr = dbRecord.split("INSERT");
        List<String> stringList = Arrays.asList(insertArr);

        return getFirstId(stringList);
    }


    private static String dbRecordToJsonStr(List<String> dbRecordList) {

        if (null == dbRecordList || dbRecordList.size() == 0) {
            return null;
        }
        StringBuilder resultBuilder = new StringBuilder();
        StringBuilder sb = null;

        for (int i = 0; i < dbRecordList.size(); i++) {

            String dbRecord = dbRecordList.get(i);
            if (!dbRecord.contains("(")) {
                continue;
            }

            String fields = dbRecord.substring(dbRecord.indexOf("(") + 1, dbRecord.indexOf(")"));
            String values = dbRecord.substring(dbRecord.lastIndexOf("(") + 1, dbRecord.lastIndexOf(")"));
            String replacedFields = fields.replace("`", "");
            String replacedValues = values.replace("'", "").trim();
            String[] fieldsArr = replacedFields.split(",");
            String[] valuesArr = replacedValues.split(",");
            sb = new StringBuilder();
            for (int j = 0; j < fieldsArr.length; j++) {
                if (0 == j) {
                    sb.append("{").
                            append("\"").
                            append(fieldsArr[j].trim()).
                            append("\"").
                            append(":").
                            append("\"").append(valuesArr[j].trim()).
                            append("\"").
                            append(",");
                } else {
                    sb.append("\"").
                            append(fieldsArr[j].trim()).
                            append("\"").
                            append(":").
                            append("\"").append(valuesArr[j].trim()).
                            append("\"").
                            append(",");
                }
            }
            if (i != dbRecordList.size() - 1) {
                resultBuilder.append(sb.substring(0, sb.lastIndexOf(","))).append("},");
            } else {
                resultBuilder.append(sb.substring(0, sb.lastIndexOf(","))).append("}");
            }
        }

        return resultBuilder.toString();
    }


    private static String getFirstId(List<String> dbRecordList) {

        if (null == dbRecordList || dbRecordList.size() == 0) {
            return null;
        }
        String firstId="";

        for (int i = 0; i < dbRecordList.size(); i++) {

            String dbRecord = dbRecordList.get(i);
            if (!dbRecord.contains("(")) {
                continue;
            }

            String fields = dbRecord.substring(dbRecord.indexOf("(") + 1, dbRecord.indexOf(")"));
            String values = dbRecord.substring(dbRecord.lastIndexOf("(") + 1, dbRecord.lastIndexOf(")"));
            String replacedFields = fields.replace("`", "");
            String replacedValues = values.replace("'", "").trim();
            String[] valuesArr = replacedValues.split(",");
            firstId = valuesArr[0];
            break;
        }
        return firstId;
    }
}
