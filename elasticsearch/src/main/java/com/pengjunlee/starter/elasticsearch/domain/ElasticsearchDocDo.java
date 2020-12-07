package com.pengjunlee.starter.elasticsearch.domain;

import lombok.Data;

/**
 * [简要描述]: 索引文档数据结构
 * [详细描述]:
 *
 * @author pengjunlee
 * @create 2020-12-02 15:32
 */
@Data
public class ElasticsearchDocDo
{
    /**
     * 索引
     */
    private String index;
    /**
     * 文档ID
     */
    private String id;
    /**
     * 路由参数
     */
    private String routing;
    /**
     * 文档内容
     */
    private String jsonDoc;

    public ElasticsearchDocDo()
    {

    }

    public ElasticsearchDocDo(String index, String id, String routing, String jsonDoc)
    {
        this.index = index;
        this.id = id;
        this.routing = routing;
        this.jsonDoc = jsonDoc;
    }

    public ElasticsearchDocDo(String index, String id, String jsonDoc)
    {
        this.index = index;
        this.id = id;
        this.jsonDoc = jsonDoc;
    }
}
