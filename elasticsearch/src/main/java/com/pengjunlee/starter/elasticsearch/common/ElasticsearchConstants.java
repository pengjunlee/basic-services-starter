package com.pengjunlee.starter.elasticsearch.common;

import org.elasticsearch.common.settings.Settings;

/**
 * [简要描述]: ES默认配置常量
 * [详细描述]:
 *
 * @author pengjunlee
 * @create 2020-12-02 15:32
 */
public interface ElasticsearchConstants {

    /**
     * 别名后缀
     */
    String ALIAS_SUFFIX = "-alias";

    /**
     * 200响应
     */
    int HTTP_200 = 200;

    /**
     * 4xx错误
     */
    int HTTP_400 = 400;

    /**
     * 5xx 错误
     */
    int HTTP_500 = 500;

    /**
     * 分页最大数量 100
     */
    int MAX_PAGE_SIZE = 100;

    /**
     * ES 索引分片数量参数值
     */
    int NUMBER_OF_SHARDS = 3;

    /**
     * ES 索引默认副本数量值
     */
    int NUMBER_OF_REPLICAS = 2;

    /**
     * ES 索引分片数量参数
     */
    String INDEX_NUMBER_OF_SHARDS = "index.number_of_shards";

    /**
     * ES 索引默认副本数量
     */
    String INDEX_NUMBER_OF_REPLICAS = "index.number_of_replicas";

    /**
     * 默认setting配置<p>
     * index.number_of_shards:3<p>
     * index.number_of_replicas:2<p>
     */
    Settings.Builder DEFAULT_SETTINGS = Settings.builder().put(INDEX_NUMBER_OF_SHARDS, NUMBER_OF_SHARDS)
            .put(INDEX_NUMBER_OF_REPLICAS, NUMBER_OF_REPLICAS);

}
