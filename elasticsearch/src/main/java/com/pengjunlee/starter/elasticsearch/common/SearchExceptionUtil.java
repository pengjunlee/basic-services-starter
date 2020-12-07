package com.pengjunlee.starter.elasticsearch.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.StatusLine;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.ResponseException;

/**
 * [简要描述]:
 * [详细描述]:
 *
 * @author pengjunlee
 * @create 2020-12-02 15:32
 */
@Slf4j
public class SearchExceptionUtil {
    /**
     * [简要描述]:异常详情<br/>
     * [详细描述]:<br/>
     *
     * @param e :
     * @return void
     **/
    public static void exceptionDetail(Exception e, ElasticsearchOperationEnum operationEnum) {
        if (e instanceof ResponseException) {
            ResponseException re = (ResponseException) e;
            final StatusLine statusLine = re.getResponse().getStatusLine();
            int status = statusLine.getStatusCode();
            String reasonPhrase = statusLine.getReasonPhrase();
            statusInfo(status, reasonPhrase);
        }

        if (e instanceof ElasticsearchException) {
            ElasticsearchException elasticsearchException = (ElasticsearchException) e;
            int status = elasticsearchException.status().getStatus();
            String detailedMessage = elasticsearchException.getDetailedMessage();
            statusInfo(status, detailedMessage);
        }
        log.error("请求ES:{}操作发生异常，错误详情：", operationEnum.getOperationName(), e);
    }

    private static void statusInfo(int status, String detailedMessage) {
        log.error("请求ES操作发生服务端错误，错误状态码：{},错误消息：{}", status, detailedMessage);
    }
}
