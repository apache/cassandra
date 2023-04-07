package org.apache.cassandra.audit.es;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.cassandra.audit.es.common.ErrorEnum;
import org.apache.cassandra.audit.es.dto.EsResDto;
import org.apache.cassandra.audit.es.dto.Hites;
import org.apache.cassandra.audit.es.res.DataRsp;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpUtil {

    /**
     * 创建索引
     *
     * @param url
     * @param indexName
     * @param json
     * @param id
     * @return
     */
    public static DataRsp createIndex(String url, String indexName, String json, String id) {
        String nodeUrl = getRandomNode(url);
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return DataRsp.getError406();
        }
        Unirest.setTimeouts(0, 0);
        try {
            HttpResponse<String> response = Unirest.put(nodeUrl + "/" + indexName + "/_doc/" + id)
                    .header("Content-Type", "application/json")
                    .body(json)
                    .asString();


            System.out.println("Insert 数据返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());

            if (response.getStatus() != ErrorEnum.SUCCESS.code) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .build();
            }
        } catch (UnirestException e) {
            e.printStackTrace();
        }

        return DataRsp.getError200();
    }


    public static DataRsp bulkIndex(String url, String indexName, Map<String, Object> maps) {
        String nodeUrl = getRandomNode(url);
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return DataRsp.getError406();
        }
        Unirest.setTimeouts(0, 0);
        try {
            String bulkApiJson = EsUtil.getBulkCreateApiJson(maps);
            HttpResponse<String> response = Unirest.post(nodeUrl+"/"+indexName+"/_bulk")
                    .header("Content-Type", "application/x-ndjson")
                    .body(bulkApiJson)
                    .asString();
            System.out.println("Bulk 数据返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return DataRsp.getError200();
    }


    public static DataRsp bulkUpdate(String url,String indexName,Map<String,Object> maps,String docId){
        String nodeUrl = getRandomNode(url);
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return DataRsp.getError406();
        }
        Unirest.setTimeouts(0, 0);
        try {
            String bulkApiJson = EsUtil.getBulkUpdateApiJson(maps,docId);
            HttpResponse<String> response = Unirest.post(nodeUrl+"/"+indexName+"/_bulk")
                    .header("Content-Type", "application/x-ndjson")
                    .body(bulkApiJson)
                    .asString();
            System.out.println("Bulk 数据返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return DataRsp.getError200();
    }

    /**
     * 查询方法,match  keyword
     * 多条件 使用bool 查询
     *
     * @param url
     * @param indexName
     * @return
     */
    public static DataRsp<Object> getSearch(String url, String indexName, Map<String, Object> maps) {
        String nodeUrl = getRandomNode(url);
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return DataRsp.getError406();
        }
        List<Hites> hitesList = new ArrayList<>();
        Unirest.setTimeouts(0, 0);
        try {

            String requestJson = EsUtil.getDslQueryJson(maps);
            System.out.println("查询 DSL：" + requestJson);
            HttpResponse<String> response = Unirest.post(nodeUrl + "/" + indexName + "/_search")
                    .header("Content-Type", "application/json")
                    .body(requestJson)
                    .asString();
            if (response.getStatus() != ErrorEnum.SUCCESS.code) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .data(hitesList)
                        .build();
            }
            System.out.println("结果1：" + response.getBody());
            EsResDto esResDto = JSONObject.parseObject(response.getBody(), EsResDto.class);
            System.out.println("结果2：" + JSON.toJSONString(esResDto));
            hitesList = esResDto.getHits().getHits();

        } catch (UnirestException e) {
            e.printStackTrace();
        }

        return DataRsp.builder().code(ErrorEnum.SUCCESS.code)
                .message(ErrorEnum.SUCCESS.message)
                .data(hitesList)
                .build();
    }

    /**
     * 获取索引是否存在
     * 通过ID 查询
     *
     * @param url
     * @param indexName
     * @param id
     * @return 非200都是不存在
     */
    public static DataRsp<Object> getIndex(String url, String indexName, String id) {
        String nodeUrl = getRandomNode(url);
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return DataRsp.getError406();
        }
        Unirest.setTimeouts(0, 0);
        try {
            HttpResponse<String> response = Unirest.get(nodeUrl + "/" + indexName + "/_doc/" + id)
                    .header("Content-Type", "application/json")
                    .asString();

            if (ErrorEnum.SUCCESS.code != response.getStatus()) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .build();
            }
        } catch (Exception e) {
            System.out.print("LEI TEST ERROR:");
            throw new RuntimeException(e);
        }
        return DataRsp.getError200();
    }

    /**
     * 删除数据
     *
     * @param url
     * @param indexName
     * @param maps
     * @return
     */
    public static DataRsp deleteData(String url, String indexName, Map maps) {
        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            return DataRsp.getError406();
        }
        Unirest.setTimeouts(0, 0);
        try {
            String requestJson = EsUtil.getDslQueryJson(maps);
            System.out.println("删除 DSL：" + requestJson);
            HttpResponse<String> response = Unirest.post(nodeUrl + "/" + indexName + "/_delete_by_query?slices=auto&conflicts=proceed&wait_for_completion=false")
                    .header("Content-Type", "application/json")
                    .body(requestJson)
                    .asString();
            System.out.println("删除返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());
            if (ErrorEnum.SUCCESS.code != response.getStatus()) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .build();
            }
        } catch (Exception e) {
            System.out.print("LEI TEST ERROR:");
            throw new RuntimeException(e);
        }
        return DataRsp.getError200();
    }


    public static String getRandomNode(String esNodeList) {
        if (StringUtils.isBlank(esNodeList)) {
            System.out.println("LEI TEST WARN :es_node_list 配置为空,");
            return "";
        }
        String[] nodeList = esNodeList.split(",");
        int index = (int) (Math.random() * nodeList.length);
        return nodeList[index];
    }

    /**
     * 删除索引
     *
     * @param url
     * @param indexName
     * @return
     */
    public static DataRsp dropIndex(String url, String indexName) {

        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            return DataRsp.getError406();
        }
        Unirest.setTimeouts(0, 0);
        try {

            HttpResponse<String> response = Unirest.delete(nodeUrl + "/" + indexName)
                    .asString();

            System.out.println("删除索引返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());
            if (ErrorEnum.SUCCESS.code != response.getStatus()) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .build();
            }

        } catch (Exception e) {
            System.out.print("LEI TEST ERROR:");
            throw new RuntimeException(e);
        }
        return DataRsp.getError200();
    }
}
