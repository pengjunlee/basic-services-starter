package com.pengjunlee.starter.elasticsearch.service.search;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Map;

public interface EsService {
    /**
     * [简要描述]: 根据索引和条件查询
     * [详细描述]:
     *
     * @param index      : 索引
     * @param conditions : 条件
     * @return org.elasticsearch.action.search.SearchResponse
     **/
    SearchResponse searchDoc(String index, Map<String, List<String>> conditions);

    SearchResponse searchDoc(String index, SearchSourceBuilder searchSourceBuilder);

    /**
     * [简要描述]: 根据索引和条件和返回对象查询
     * [详细描述]:
     *
     * @param index      : 索引
     * @param conditions : 条件
     * @param clazz      : 返回文档对象
     * @return java.util.List<T>
     **/
    <T> List<T> searchDoc(String index, Map<String, List<String>> conditions, Class<T> clazz);

    <T> List<T> searchAllDoc(String index, Map<String, List<String>> conditions, Class<T> clazz);

    long searchAllDocNum(String index, SearchSourceBuilder searchSourceBuilder);

    /**
     * [简要描述]: 批量添加文档
     * [详细描述]:
     *
     * @param index : 索引
     * @param docs  : 文档集合
     * @return boolean
     **/
    boolean addDatas(String index, List<JSONObject> docs);

    /**
     * [简要描述]: 根据文档id单个删除文档
     * [详细描述]:
     *
     * @param id    : 文档id
     * @param index : 索引
     * @return boolean
     **/
    boolean deleteData(String id, String index);

    /**
     * [简要描述]: 根据条件批量删除
     * [详细描述]:
     *
     * @param index      : 索引
     * @param conditions : 删除条件
     * @return boolean
     **/
    boolean deleteDatasByCommodityNo(String index, Map<String, List<String>> conditions);

    /**
     * [简要描述]: 批量更新文档
     * [详细描述]:
     *
     * @param index      : 索引
     * @param updateDocs : 需要更新的文档
     * @return void
     **/
    void beachUpdateDoc(String index, Map<String, String> updateDocs);
}
