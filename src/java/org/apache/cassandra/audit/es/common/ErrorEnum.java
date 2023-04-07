package org.apache.cassandra.audit.es.common;

public enum ErrorEnum {
    SUCCESS(200, "成功"),
    FAILURE(1000, "发送错误"),
    CUSTOM_ERROR(1001, "自定义错误"),
    ES_URL_NOT_FOND(406,"ES 节点未配置")
    ;

    public int code;
    public String message;

    ErrorEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }
}

