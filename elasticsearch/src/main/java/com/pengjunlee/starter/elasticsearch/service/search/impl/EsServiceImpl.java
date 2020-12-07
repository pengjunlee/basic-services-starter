package com.pengjunlee.starter.elasticsearch.service.search.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pengjunlee.starter.elasticsearch.common.ElasticsearchConstants;
import com.pengjunlee.starter.elasticsearch.common.ElasticsearchOperationEnum;
import com.pengjunlee.starter.elasticsearch.common.SearchExceptionUtil;
import com.pengjunlee.starter.elasticsearch.service.search.EsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * [简要描述]: es操作
 * [详细描述]:
 *
 * @author pengjunlee
 * @create 2020-12-02 15:32
 */
@Slf4j
@Service
public class EsServiceImpl implements EsService {
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Override
    public SearchResponse searchDoc(String index, Map<String, List<String>> conditions) {
        if (StringUtils.isEmpty(index) || CollectionUtils.isEmpty(conditions)) {
            log.error("根据条件获取文档失败，请求参数索引：【{}】，参数：【{}】不能为空!", index, JSONObject.toJSONString(conditions));
            return null;
        }

        index = index.toLowerCase();
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.from(0);
        searchSourceBuilder.size(1000);

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        conditions.forEach((key, value) -> queryBuilder.must(QueryBuilders.termsQuery(key, value)));
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = null;

        try {
            search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("根据查询条件【{}】获取文档出现异常", JSON.toJSON(conditions), e);
        }
        return search;
    }

    @Override
    public SearchResponse searchDoc(String index, SearchSourceBuilder searchSourceBuilder) {
        if (StringUtils.isEmpty(index) || null == searchSourceBuilder) {
            log.error("根据条件获取文档失败，请求参数索引：【{}】，参数不能为空!", index);
            return null;
        }

        index = index.toLowerCase();
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = null;

        try {
            search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("根据查询条件获取文档出现异常", e);
        }
        return search;
    }

    @Override
    public <T> List<T> searchDoc(String index, Map<String, List<String>> conditions, Class<T> clazz) {
        if (StringUtils.isBlank(index) || CollectionUtils.isEmpty(conditions) || Objects.isNull(clazz)) {
            log.error("根据条件获取文档失败，请求参数索引：【{}】，参数：【{}}，返回类型：【{}】不能为空!", index, JSONObject
                    .toJSONString(conditions), clazz);
            return null;
        }

        SearchResponse search = this.searchDoc(index, conditions);

        List<T> docList = new ArrayList<>();
        if (null != search) {
            SearchHits hits = search.getHits();
            if (search.status() == RestStatus.OK) {
                if (hits.getTotalHits().value > 0) {
                    hits.forEach(hit -> docList.add(JSON.parseObject(hit.getSourceAsString(), clazz)));
                }
            } else {
                log.error("搜索返回状态码:{}", search.status().getStatus());
            }
        }
        return docList;
    }

    @Override
    public <T> List<T> searchAllDoc(String index, Map<String, List<String>> conditions, Class<T> clazz) {
        if (StringUtils.isBlank(index) || CollectionUtils.isEmpty(conditions) || Objects.isNull(clazz)) {
            log.error("根据条件获取文档失败，请求参数索引：【{}】，参数：【{}}，返回类型：【{}】不能为空!", index, JSONObject
                    .toJSONString(conditions), clazz);
            return null;
        }

        List<T> docList = new ArrayList<>();
        SearchSourceBuilder searchSourceBuilder = this.disposeConditions(conditions);
        if (null != searchSourceBuilder) {
            long docNum = this.searchAllDocNum(index, searchSourceBuilder);

            int from = 0;
            int size = ElasticsearchConstants.MAX_PAGE_SIZE;
            long fromSize = docNum / size;
            while ((fromSize - from) >= 0) {
                searchSourceBuilder.from(from * size);
                searchSourceBuilder.size(size);
                SearchResponse search = this.searchDoc(index, searchSourceBuilder);
                if (null != search) {
                    SearchHits hits = search.getHits();
                    if (search.status() == RestStatus.OK) {
                        if (hits.getTotalHits().value > 0) {
                            hits.forEach(hit -> docList.add(JSON.parseObject(hit.getSourceAsString(), clazz)));
                        }
                    } else {
                        log.error("搜索返回状态码:{}", search.status().getStatus());
                        break;
                    }
                } else {
                    log.warn("当前未查询到文档信息, 结束本次查询，【from: {}; size: {}】", from, size);
                    break;
                }
                from++;
            }
        }
        return docList;
    }

    @Override
    public long searchAllDocNum(String index, SearchSourceBuilder searchSourceBuilder) {
        if (StringUtils.isEmpty(index) || null == searchSourceBuilder) {
            log.error("根据条件获取文档失败，请求参数索引：【{}】或者参数不能为空!", index);
            return 0;
        }

        index = index.toLowerCase();
        CountRequest countRequest = new CountRequest(index);
        countRequest.source(searchSourceBuilder);

        long count = 0;
        try {
            count = restHighLevelClient.count(countRequest, RequestOptions.DEFAULT).getCount();
        } catch (IOException e) {
            log.error("根据查询条件【{}】获取文档出现异常", searchSourceBuilder.toString(), e);
        }
        return count;
    }

    @Override
    public boolean addDatas(String index, List<JSONObject> docs) {
        if (CollectionUtils.isEmpty(docs)) {
            log.error("Parameter  is null  in ManagerService addDatas() method");
            return false;
        }

        BulkRequest bulkRequest = new BulkRequest();
        int result = 0;
        String indexId;
        for (JSONObject doc : docs) {
            indexId = String.valueOf(doc.get("id"));
            if (StringUtils.isBlank(index) || StringUtils.isBlank(indexId)) {
                log.warn("批量添加的当前数据索引或ID为空，调过该条数据：{}", JSONObject.toJSONString(doc));
            } else {
                IndexRequest indexRequest = new IndexRequest(index.toLowerCase());
                indexRequest.id(indexId);
                indexRequest.source(JSONObject.toJSONString(doc), XContentType.JSON);
                bulkRequest.add(indexRequest);
                result++;
            }
        }
        log.info("批量添加数据，添加总量：{}", docs.size());

        if (result > 0) {
            final int count = result;

            restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    int failed = 0;
                    // 有失败的
                    if (bulkItemResponses.hasFailures()) {
                        for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                            if (bulkItemResponse.isFailed()) {
                                failed++;
                                log.error("索引：{}下的文档ID为：{}添加失败", bulkItemResponse.getIndex(), bulkItemResponse.getId());
                            }
                        }
                    }
                    log.info("批量添加文档，需要插入的正确文档总数量：{}，其中添加成功的文档数量：{}，添加失败的文档数量：{}", count, count - failed, failed);
                }

                @Override
                public void onFailure(Exception e) {
                    log.error("批量添加文档数据出现异常", e);
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.ADD_DOCUMENT);
                }
            });
            return true;
        }
        return true;
    }

    @Override
    public boolean deleteData(String id, String index) {
        if (StringUtils.isBlank(id) || StringUtils.isBlank(index)) {
            log.error("删除失败，索引名称和文档ID不能为空!");
            return false;
        }
        final String docIndex = index.toLowerCase();
        final String docId = id;
        DeleteRequest deleteRequest = new DeleteRequest(docIndex, docId);
        restHighLevelClient.deleteAsync(deleteRequest, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                log.info("索引：{}下删除文档ID：{}返回状态码：{}", docIndex, docId, deleteResponse.status());
            }

            @Override
            public void onFailure(Exception e) {
                log.error("批量添加文档数据出现异常", e);
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.DELETE_DOCUMENT);
            }
        });
        return true;
    }

    @Override
    public boolean deleteDatasByCommodityNo(String index, Map<String, List<String>> conditions) {
        if (StringUtils.isEmpty(index) || CollectionUtils.isEmpty(conditions)) {
            log.error("根据条件删除文档失败，请求参数索引：【{}】，参数：【{}】不能为空!", index, JSONObject.toJSONString(conditions));
            return false;
        }

        boolean result = false;
        SearchResponse searchResponse = this.searchDoc(index, conditions);
        if (null != searchResponse) {
            if (RestStatus.OK == searchResponse.status()) {
                SearchHits hits = searchResponse.getHits();
                if (0 < hits.getTotalHits().value) {
                    List<String> docIds = new ArrayList<>(hits.getHits().length);
                    hits.forEach(hit -> docIds.add(hit.getId()));

                    BulkRequest bulkRequest = new BulkRequest();
                    docIds.forEach(docid ->
                    {
                        DeleteRequest deleteRequest = new DeleteRequest(index, docid);
                        bulkRequest.add(deleteRequest);
                    });

                    BulkResponse bulk = null;
                    try {
                        bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    } catch (IOException e) {
                        log.error("批量删除文档失败，文档id【{}】", JSON.toJSON(docIds), e);
                    }

                    if (null != bulk) {
                        if (bulk.hasFailures()) {
                            log.error("根据文档id集合【{}】批量删除文档数据失败【{}】", JSON.toJSON(docIds), bulk.buildFailureMessage());
                        } else {
                            result = true;
                        }
                    }
                }
            } else {
                log.error("根据条件搜索需要删除的文档时返回状态码:{}", searchResponse.status().getStatus());
            }
        }
        return result;
    }

    @Override
    public void beachUpdateDoc(String index, Map<String, String> updateDocs) {
        if (StringUtils.isEmpty(index) || CollectionUtils.isEmpty(updateDocs)) {
            log.error("批量更新文档失败，请求参数索引：【{}】，参数：【{}】不能为空!", index, JSONObject.toJSONString(updateDocs));
            return;
        }

        final String newIndex = index.toLowerCase();
        final BulkRequest bulkRequest = new BulkRequest();
        updateDocs.forEach((key, value) -> bulkRequest
                .add(new UpdateRequest(newIndex, key).doc(value, XContentType.JSON)));

        try {
            restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("批量更新文档id【{}】失败", updateDocs.keySet(), e);
            SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.SEARCH);
        }
    }

    private SearchSourceBuilder disposeConditions(Map<String, List<String>> conditions) {
        if (CollectionUtils.isEmpty(conditions)) {
            log.warn("根据条件【{}】处理查询条件失败，条件不能为空", JSON.toJSON(conditions));
            return null;
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        conditions.forEach((key, value) -> queryBuilder.must(QueryBuilders.termsQuery(key, value)));
        searchSourceBuilder.query(queryBuilder);
        return searchSourceBuilder;
    }

}