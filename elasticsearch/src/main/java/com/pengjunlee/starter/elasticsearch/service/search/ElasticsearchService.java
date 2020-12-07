package com.pengjunlee.starter.elasticsearch.service.search;


import com.pengjunlee.starter.elasticsearch.domain.ElasticSearchRequestDo;
import com.pengjunlee.starter.elasticsearch.domain.ElasticSearchResultDo;
import com.pengjunlee.starter.elasticsearch.domain.ElasticsearchDocDo;
import com.pengjunlee.starter.elasticsearch.domain.PaginationDo;

import java.util.List;

/**
 * [简要描述]: ES服务
 * [详细描述]:
 *
 * @author pengjunlee
 * @create 2020-12-02 15:32
 */
public interface ElasticsearchService {

    /**
     * [简要描述]:
     * [详细描述]:
     *
     * @param docData :
     * @param async   :
     * @return boolean
     **/
    boolean existDocById(ElasticsearchDocDo docData, boolean async);

    /**
     * [简要描述]:主键ID查找文档
     * [详细描述]:
     *
     * @param index     : 索引
     * @param id        : 文档ID
     * @param resultCls : 返回类型
     * @return java.lang.Object
     **/
    <T> T getById(String index, String id, Class<T> resultCls);

    /**
     * [简要描述]:搜索文档
     * [详细描述]:
     *
     * @param searchRequestDo : 搜索请求
     * @return org.elasticsearch.action.search.SearchResponse
     **/
    <T> PaginationDo<ElasticSearchResultDo<T>> search(ElasticSearchRequestDo<T> searchRequestDo);

    /**
     * [简要描述]:添加文档
     * [详细描述]:
     *
     * @param docDo: 文档
     * @param async: 是否异步
     * @return boolean
     **/
    boolean addJsonDoc(ElasticsearchDocDo docDo, boolean async);

    /**
     * [简要描述]:批量添加文档
     * [详细描述]:
     *
     * @param docDataList : 批量数据
     * @param async       : 是否异步
     * @return boolean
     **/
    boolean addJsonDoc(List<ElasticsearchDocDo> docDataList, boolean async);

    /**
     * [简要描述]:文档ID单个更新
     * [详细描述]:
     *
     * @param updateDoc : 待更新的文档
     * @param async     : 是否异步
     * @return boolean
     **/
    boolean updateDoc(ElasticsearchDocDo updateDoc, boolean async);

    /**
     * [简要描述]:文档ID批量更新
     * [详细描述]:
     *
     * @param updateDocList : 待更新的文档集
     * @param async         : 是否异步
     * @return boolean
     **/
    boolean batchUpdateDoc(List<ElasticsearchDocDo> updateDocList, boolean async);

    /**
     * [简要描述]:ID 删除文档
     * [详细描述]:
     *
     * @param id     : ID
     * @param index  : 索引
     * @param async: 是否异步
     * @return boolean
     **/
    boolean deleteById(String id, String index, boolean async);

    /**
     * [简要描述]:
     * [详细描述]:
     *
     * @param deleteDoc :
     * @param async     :
     * @return boolean
     **/
    boolean deleteById(ElasticsearchDocDo deleteDoc, boolean async);

    /**
     * [简要描述]:批量删除
     * [详细描述]:
     *
     * @param deleteDocs : 待删除的文档
     * @param async      : 是否异步
     * @return boolean
     **/
    boolean batchDeleteById(List<ElasticsearchDocDo> deleteDocs, boolean async);

    /**
     * [简要描述]:依据条件查询总数
     * [详细描述]:
     *
     * @param elasticSearchRequestDo : 查询条件
     * @return long
     **/
    <T> long count(ElasticSearchRequestDo<T> elasticSearchRequestDo);

    /**
     * [简要描述]:字段聚合
     * [详细描述]:
     *
     * @param elasticSearchRequestDo :
     * @return java.util.List<java.lang.String>
     **/
    <T> List<String> aggregations(ElasticSearchRequestDo<T> elasticSearchRequestDo);

    /**
     * 获取keyword 分词结果
     *
     * @param keyword 关键字
     * @param index   索引名
     * @param field   字段名
     */
    List<String> getIkList(String keyword, String index, String field);

}
