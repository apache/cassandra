package org.apache.cassandra.audit.es;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.lang3.StringUtils;

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
    public static int createIndex(String url, String indexName, String json, String id) {
        String nodeUrl = getRandomNode(url);
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return 406;
        }
        int code = 200;
        Unirest.setTimeouts(0, 0);
        try {
            HttpResponse<String> response = Unirest.put(nodeUrl + "/" + indexName + "/_doc/" + id)
                    .header("Content-Type", "application/json")
                    .body(json)
                    .asString();

            if (response.getStatus() != code) {
                return response.getStatus();
            }
        } catch (UnirestException e) {
            e.printStackTrace();
        }

        return code;
    }

    /**
     * 获取索引是否存在
     *
     * @param url
     * @param indexName
     * @param id
     * @return 非200都是不存在
     */
    public static int getIndex(String url, String indexName, String id) {
        String nodeUrl = getRandomNode(url);
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return 406;
        }
        Unirest.setTimeouts(0, 0);
        int code = 200;
        try {
            HttpResponse<String> response = Unirest.get(nodeUrl + "/" + indexName + "/_doc/" + id)
                    .header("Content-Type", "application/json")
                    .asString();

            System.out.println(response.getStatus());
            System.out.println(response.getStatusText());
            System.out.println(response.getBody());
            System.out.println("--------------");

            if (code != response.getStatus()) {
                return response.getStatus();
            }
        } catch (Exception e) {
            System.out.print("LEI TEST ERROR:");
            throw new RuntimeException(e);
        }
        return code;
    }


    private static String getRandomNode(String esNodeList) {
        if (StringUtils.isBlank(esNodeList)) {
            System.out.println("LEI TEST WARN :es_node_list 配置为空,");
            return "";
        }
        String[] nodeList = esNodeList.split(",");
        int index = (int) (Math.random() * nodeList.length);
        return nodeList[index];
    }


}
