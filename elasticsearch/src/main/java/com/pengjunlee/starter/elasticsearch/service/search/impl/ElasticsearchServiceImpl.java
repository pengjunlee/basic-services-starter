package com.pengjunlee.starter.elasticsearch.service.search.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pengjunlee.starter.elasticsearch.common.ElasticsearchConstants;
import com.pengjunlee.starter.elasticsearch.common.ElasticsearchOperationEnum;
import com.pengjunlee.starter.elasticsearch.common.SearchExceptionUtil;
import com.pengjunlee.starter.elasticsearch.domain.ElasticSearchRequestDo;
import com.pengjunlee.starter.elasticsearch.domain.ElasticSearchResultDo;
import com.pengjunlee.starter.elasticsearch.domain.ElasticsearchDocDo;
import com.pengjunlee.starter.elasticsearch.domain.PaginationDo;
import com.pengjunlee.starter.elasticsearch.service.search.ElasticsearchService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.*;

/**
 * [简要描述]: Elasticsearch 服务
 * [详细描述]:
 *
 * @author pengjunlee
 * @create 2020-12-02 15:32
 */
@Service
@Slf4j
public class ElasticsearchServiceImpl implements ElasticsearchService, ElasticsearchConstants
{
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Override
    public boolean existDocById(ElasticsearchDocDo docData, boolean async)
    {
        if (null == docData || StringUtils.isBlank(docData.getIndex()) || StringUtils.isBlank(docData.getId()))
        {
            log.error("判断文档是否存在失败，参数必须存在index和文档id！参数内容【{}】", JSONObject.toJSONString(docData));
            return false;
        }
        String index = docData.getIndex().toLowerCase();
        String id = docData.getId();
        String routing = docData.getRouting();
        GetRequest getRequest = new GetRequest(index);
        if (StringUtils.isNotBlank(id))
        {
            getRequest.id(id);
        }
        if (StringUtils.isNotBlank(routing))
        {
            getRequest.routing(routing);
        }
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        if (async)
        {
            restHighLevelClient.existsAsync(getRequest, RequestOptions.DEFAULT, new ActionListener<Boolean>()
            {
                @Override
                public void onResponse(Boolean exists)
                {
                    if (!exists)
                    {
                        log.error("判断索引:【{}】文档id：【{}】是否存在失败",index,id);
                    }
                }

                @Override
                public void onFailure(Exception e)
                {
                    log.error("判断索引：【{}】文档id：【{}】是否存在异常！", index, id);
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.ADD_DOCUMENT);
                }
            });
            return true;
        }
        else
        {
            try
            {
               return restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);

            }
            catch (Exception e)
            {
                log.error("判断索引：【{}】文档id：【{}】是否存在异常！", index, id);
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.ADD_DOCUMENT);
                return false;
            }
        }

    }

    /**
     * [简要描述]:文档ID，获取文档
     * [详细描述]:如果出现异常返回null,调用者需要处理null
     *
     * @param index : 索引
     * @param id: 主键ID
     * @param resultCls: 返回类型
     * @return org.elasticsearch.action.get.GetResponse
     **/
    @Override
    public <T> T getById(String index, String id, Class<T> resultCls)
    {
        if (StringUtils.isBlank(index) || StringUtils.isBlank(id) || Objects.isNull(resultCls))
        {
            log.error("文档ID获取文档失败，请求参数索引：{}，ID：{}，返回类型：{}不能为空!", index, id, resultCls);
            return null;
        }
        try
        {
            GetRequest getRequest = new GetRequest(index, id);
            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            if (getResponse.isExists())
            {
                return JSON.parseObject(getResponse.getSourceAsString(), resultCls);
            }
            else
            {
                log.error("主键ID：{}到索引：{}找不到文档。", id, index);
            }
        }
        catch (Exception e)
        {
            SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.SEARCH_BY_ID);
        }
        return null;
    }

    /**
     * [简要描述]:搜索文档
     * [详细描述]:如果出现异常返回null,调用者需要处理null
     *
     * @param searchRequestDo : 搜素请求
     * @return org.elasticsearch.action.search.SearchResponse
     **/
    @Override
    public <T> PaginationDo<ElasticSearchResultDo<T>> search(ElasticSearchRequestDo<T> searchRequestDo)
    {
        PaginationDo<ElasticSearchResultDo<T>> paginationDo = new PaginationDo<>();
        if (Objects.isNull(searchRequestDo) || Objects.isNull(searchRequestDo.getSearchSourceBuilder()) || Objects
                .isNull(searchRequestDo.getDocCls()) || StringUtils.isBlank(searchRequestDo.getIndex()))
        {
            log.error("搜索文档失败，请求参数不能为空!");
            // 防止调用者空指针
            paginationDo.setResults(new ArrayList<>());
            return paginationDo;
        }

        SearchRequest searchRequest = new SearchRequest(searchRequestDo.getIndex().toLowerCase());
        SearchSourceBuilder searchSourceBuilder = searchRequestDo.getSearchSourceBuilder();

        // 分页处理
        if (searchRequestDo.isUsePage())
        {
            Integer pageNo = searchRequestDo.getPageNo();
            Integer pageSize = searchRequestDo.getPageSize();
            pageNo = null == pageNo ? 1 : pageNo;
            paginationDo.setPageNo(pageNo);
            pageSize = null == pageSize ? 1 : pageSize;
            pageSize = pageSize > MAX_PAGE_SIZE ? MAX_PAGE_SIZE : pageSize;
            paginationDo.setPageSize(pageSize);

            searchSourceBuilder.from((pageNo - 1) * pageSize);
            searchSourceBuilder.size(pageSize);
        }

        if (log.isDebugEnabled())
        {
            log.debug("searchSourceBuilder:{}", searchSourceBuilder);
        }

        searchRequest.source(searchSourceBuilder);
        try
        {
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            paginationDo.setTook(response.getTook().getMillis());
            paginationDo.setTotal((int) hits.getTotalHits().value);

            List<ElasticSearchResultDo<T>> elasticSearchResultDos = processSearchResult(hits, searchRequestDo
                    .getDocCls(), searchRequestDo.getHighlightFieldList());

            paginationDo.setResults(elasticSearchResultDos);
        }
        catch (Exception e)
        {
            SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.SEARCH);
        }
        return paginationDo;
    }

    /**
     * [简要描述]:添加文档
     * [详细描述]:
     *
     * @param docData : 索引文档数据
     * @param async : 是否异步
     * @return boolean
     **/
    @Override
    public boolean addJsonDoc(ElasticsearchDocDo docData, boolean async)
    {
        if (null == docData || StringUtils.isBlank(docData.getIndex()) || StringUtils.isBlank(docData.getJsonDoc()))
        {
            log.error("添加文档失败，参数必须索引，json格式文档内容！");
            return false;
        }
        String index = docData.getIndex().toLowerCase();
        String id = docData.getId();
        String jsonDoc = docData.getJsonDoc();
        String routing = docData.getRouting();
        IndexRequest indexRequest = new IndexRequest(index);
        if (StringUtils.isNotBlank(id))
        {
            indexRequest.id(id);
        }
        if (StringUtils.isNotBlank(routing))
        {
            indexRequest.routing(routing);
        }

        indexRequest.source(jsonDoc, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

        if (async)
        {
            final String tempIndex = index;
            final String tempDoc = jsonDoc;
            restHighLevelClient.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>()
            {
                @Override
                public void onResponse(IndexResponse indexResponse)
                {
                    // 创建或更新成功
                    if (indexResponse.status() != RestStatus.OK)
                    {
                        log.error("往索引：{}添加文档：{}失败，返回的状态码：{}，返回的结果：{}", tempIndex, tempDoc, indexResponse
                                .status(), indexResponse.getResult());
                    }
                }

                @Override
                public void onFailure(Exception e)
                {
                    log.error("索引：{}添加文档：{}发生异常！", tempIndex, tempDoc);
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.ADD_DOCUMENT);
                }
            });
            return true;
        }
        else
        {
            boolean result = false;
            try
            {
                final IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
                // 创建或更新成功
                if (response.status() == RestStatus.OK || response.getResult() == DocWriteResponse.Result.CREATED
                        || response.getResult() == DocWriteResponse.Result.UPDATED)
                {
                    result = true;
                }
                else
                {
                    log.error("往索引：{}添加文档：{}失败，返回的状态码：{}，返回的结果：{}", index, jsonDoc, response.status(), response
                            .getResult());
                }
            }
            catch (Exception e)
            {
                log.error("索引：{}添加文档：{}发生异常！", index, jsonDoc);
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.ADD_DOCUMENT);
            }
            return result;
        }

    }

    /**
     * [简要描述]:批量添加文档
     * [详细描述]:
     *
     * @param docDataList : 批量数据
     * @param async : 是否异步
     * @return boolean
     **/
    @Override
    public boolean addJsonDoc(List<ElasticsearchDocDo> docDataList, boolean async)
    {
        if (CollectionUtils.isEmpty(docDataList))
        {
            log.error("批量添加文档失败，要添加的文档数量为空!");
            return false;
        }

        boolean flag = false;
        BulkRequest bulkRequest = new BulkRequest();
        docDataList.forEach(docData ->
        {
            IndexRequest indexRequest = new IndexRequest(docData.getIndex().toLowerCase());
            if (StringUtils.isNotBlank(docData.getId()))
            {
                indexRequest.id(docData.getId());
            }
            if (StringUtils.isNotBlank(docData.getRouting()))
            {
                indexRequest.routing(docData.getRouting());
            }

            indexRequest.source(docData.getJsonDoc(), XContentType.JSON);
            bulkRequest.add(indexRequest).setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        });

        if (async)
        {
            // 异步
            restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>()
            {
                @Override
                public void onResponse(BulkResponse bulkItemResponses)
                {
                    if (bulkItemResponses.hasFailures())
                    {
                        log.warn("批量添加索引文档有失败数据，可能失败原因：{}", bulkItemResponses.buildFailureMessage());
                        bulkItemResponses.forEach(bulkItemResponse ->
                        {
                            if (bulkItemResponse.isFailed())
                            {
                                log.error("索引：{}下添加文档ID：{}的文档失败，失败原因：{}", bulkItemResponse.getIndex(), bulkItemResponse
                                        .getId(), bulkItemResponse.getFailureMessage());
                            }
                        });
                    }
                }

                @Override
                public void onFailure(Exception e)
                {
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.BATCH_ADD_DOCUMENT);
                }
            });
            flag = true;
        }
        else
        {
            // 同步
            try
            {
                final BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                bulk.forEach(bulkItemResponse ->
                {
                    if (bulkItemResponse.isFailed())
                    {
                        log.error("索引：{}下添加文档ID：{}的文档失败，失败原因：{}", bulkItemResponse.getIndex(), bulkItemResponse
                                .getId(), bulkItemResponse.getFailureMessage());
                    }
                });
                flag = true;
            }
            catch (Exception e)
            {
                log.error("批量添加文档发生了异常!");
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.BATCH_ADD_DOCUMENT);
            }
        }
        return flag;
    }

    /**
     * [简要描述]:文档ID单个更新
     * [详细描述]:
     *
     * @param updateDoc : 待更新的文档
     * @param async : 是否异步
     * @return boolean
     **/
    @Override
    public boolean updateDoc(ElasticsearchDocDo updateDoc, boolean async)
    {
        if (Objects.isNull(updateDoc) || StringUtils.isBlank(updateDoc.getId()) || StringUtils
                .isBlank(updateDoc.getIndex()) || StringUtils.isBlank(updateDoc.getJsonDoc()))
        {
            log.error("文档更新失败，更新的文档参数不能为空，请求参数数据：{}", Objects.isNull(updateDoc) ?
                    "null" :
                    JSONObject.toJSONString(updateDoc));
            return false;
        }
        boolean updateFlag = false;
        UpdateRequest updateRequest = new UpdateRequest(updateDoc.getIndex().toLowerCase(), updateDoc.getId());
        updateRequest.doc(updateDoc.getJsonDoc(), XContentType.JSON);

        String routing = updateDoc.getRouting();
        if (StringUtils.isNotBlank(routing))
        {
            updateRequest.routing(routing);
        }

        if (async)
        {
            updateFlag = true;
            restHighLevelClient.updateAsync(updateRequest, RequestOptions.DEFAULT, new ActionListener<UpdateResponse>()
            {
                @Override
                public void onResponse(UpdateResponse updateResponse)
                {
                    if (updateResponse.getResult() != DocWriteResponse.Result.UPDATED)
                    {
                        log.error("文档更新出现异常，待更新的原始数据：{}", JSONObject.toJSONString(updateDoc));
                    }
                }

                @Override
                public void onFailure(Exception e)
                {
                    log.error("文档更新出现异常，待更新的原始数据：{}", JSONObject.toJSONString(updateDoc));
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.UPDATE_DOCUMENT);
                }
            });
        }
        else
        {
            try
            {
                final UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
                updateFlag = update.getResult() == DocWriteResponse.Result.UPDATED;
            }
            catch (Exception e)
            {
                log.error("文档更新出现异常，待更新的原始数据：{}", JSONObject.toJSONString(updateDoc));
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.UPDATE_DOCUMENT);
            }
        }

        return updateFlag;
    }

    /**
     * [简要描述]:文档ID批量更新
     * [详细描述]:
     *
     * @param updateDocList : 待更新的文档集
     * @param async : 是否异步
     * @return boolean
     **/
    @Override
    public boolean batchUpdateDoc(List<ElasticsearchDocDo> updateDocList, boolean async)
    {
        if (CollectionUtils.isEmpty(updateDocList))
        {
            log.error("批量更新文档失败，请求参数不能为空！");
            return false;
        }

        boolean updateFlag = false;
        BulkRequest bulkRequest = new BulkRequest();
        updateDocList.forEach(elasticsearchDocDo ->
        {
            final String id = elasticsearchDocDo.getId();
            final String index = elasticsearchDocDo.getIndex();
            final String jsonDoc = elasticsearchDocDo.getJsonDoc();
            final String routing = elasticsearchDocDo.getRouting();
            if (StringUtils.isBlank(id) || StringUtils.isBlank(index) || StringUtils.isBlank(jsonDoc))
            {
                log.error("文档更新失败，更新的文档参数不能为空，请求参数数据：{}", JSONObject.toJSONString(elasticsearchDocDo));
                return;
            }

            UpdateRequest doc = new UpdateRequest(index.toLowerCase(), id).doc(jsonDoc, XContentType.JSON);
            if (StringUtils.isNotEmpty(routing))
            {
                doc.routing(routing);
            }
            bulkRequest.add(doc);
        });

        if (async)
        {
            // 异步
            restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>()
            {
                @Override
                public void onResponse(BulkResponse bulkItemResponses)
                {
                    if (bulkItemResponses.hasFailures())
                    {
                        log.warn("批量更新文档有失败数据，可能失败原因：{}", bulkItemResponses.buildFailureMessage());
                        bulkItemResponses.forEach(bulkItemResponse ->
                        {
                            if (bulkItemResponse.isFailed())
                            {
                                log.error("索引：{}下更新文档ID：{}的文档失败，失败原因：{}", bulkItemResponse.getIndex(), bulkItemResponse
                                        .getId(), bulkItemResponse.getFailureMessage());
                            }
                        });
                    }
                }

                @Override
                public void onFailure(Exception e)
                {
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.BATCH_ADD_DOCUMENT);
                }
            });
            updateFlag = true;
        }
        else
        {
            // 同步
            try
            {
                final BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                bulk.forEach(bulkItemResponse ->
                {
                    if (bulkItemResponse.isFailed())
                    {
                        log.error("更新文档发生了失败，当前带更新的文档索引：{}，文档ID：{}，失败原因：{}", bulkItemResponse
                                .getIndex(), bulkItemResponse.getId(), bulkItemResponse.getFailureMessage());
                    }
                });
                updateFlag = true;
            }
            catch (Exception e)
            {
                log.error("更新文档发生了异常!");
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.BATCH_ADD_DOCUMENT);
            }
        }
        return updateFlag;
    }

    /**
     * [简要描述]:ID 删除文档
     * [详细描述]:
     *
     * @param id :
     * @param index :
     * @param async :
     * @return boolean
     **/
    @Override
    public boolean deleteById(String id, String index, boolean async)
    {
        if (StringUtils.isBlank(index) || StringUtils.isBlank(id))
        {
            log.error("删除文档失败，请求参数id:{},index:{}为空", id, index);
            return false;
        }
        boolean deleteFlag = false;
        DeleteRequest request = new DeleteRequest(index, id);
        if (async)
        {
            restHighLevelClient.deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>()
            {
                @Override
                public void onResponse(DeleteResponse deleteResponse)
                {
                    if (deleteResponse.status() != RestStatus.OK)
                    {
                        log.error("删除文档出现错误，索引：{}，文档ID：{}，返回状态吗：{}", index, id, deleteResponse.status().getStatus());
                    }
                }

                @Override
                public void onFailure(Exception e)
                {
                    log.error("删除文档出现错误，索引：{}，文档ID：{}", index, id);
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.DELETE_DOCUMENT);
                }
            });
            deleteFlag = true;
        }
        else
        {
            try
            {
                DeleteResponse deleteResponse = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
                deleteFlag = deleteResponse.status() == RestStatus.OK;
            }
            catch (Exception e)
            {
                log.error("删除文档出现错误，索引：{}，文档ID：{}", index, id);
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.DELETE_DOCUMENT);
            }
        }
        return deleteFlag;
    }

    @Override
    public boolean deleteById(ElasticsearchDocDo deleteDoc, boolean async)
    {
        if (Objects.isNull(deleteDoc) || StringUtils.isBlank(deleteDoc.getId()) || StringUtils
                .isBlank(deleteDoc.getIndex()))
        {
            log.error("文档删除失败，删除的文档参数不能为空，请求参数数据：{}", Objects.isNull(deleteDoc) ?
                    "null" :
                    JSONObject.toJSONString(deleteDoc));
            return false;
        }
        String index = deleteDoc.getIndex().toLowerCase();
        String id = deleteDoc.getId();
        String routing = deleteDoc.getRouting();

        boolean deleteFlag = false;
        DeleteRequest request = new DeleteRequest(index, id);
        if (StringUtils.isNotEmpty(routing))
        {
            request.routing(routing);
        }

        if (async)
        {
            restHighLevelClient.deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>()
            {
                @Override
                public void onResponse(DeleteResponse deleteResponse)
                {
                    if (deleteResponse.status() != RestStatus.OK)
                    {
                        log.error("删除文档出现错误，索引：{}，文档ID：{}，返回状态吗：{}", index, id, deleteResponse.status().getStatus());
                    }
                }

                @Override
                public void onFailure(Exception e)
                {
                    log.error("删除文档出现错误，索引：{}，文档ID：{}", index, id);
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.DELETE_DOCUMENT);
                }
            });
            deleteFlag = true;
        }
        else
        {
            try
            {
                DeleteResponse deleteResponse = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
                deleteFlag = deleteResponse.status() == RestStatus.OK;
            }
            catch (Exception e)
            {
                log.error("删除文档出现错误，索引：{}，文档ID：{}", index, id);
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.DELETE_DOCUMENT);
            }
        }
        return deleteFlag;

    }

    /**
     * [简要描述]:批量删除
     * [详细描述]:
     *
     * @param deleteDocs : 待删除的文档
     * @param async : 是否异步
     * @return boolean
     **/
    @Override
    public boolean batchDeleteById(List<ElasticsearchDocDo> deleteDocs, boolean async)
    {
        if (CollectionUtils.isEmpty(deleteDocs))
        {
            log.error("批量删除文档失败，请求的文档列表为空!");
            return false;
        }

        BulkRequest bulkRequest = new BulkRequest();
        ///        List<ElasticsearchDocDo> errorDocs = new ArrayList<>();
        deleteDocs.forEach(elasticsearchDocDo ->
        {
            final String index = elasticsearchDocDo.getIndex();
            final String id = elasticsearchDocDo.getId();
            if (StringUtils.isBlank(index) || StringUtils.isBlank(id))
            {
                ///               errorDocs.add(elasticsearchDocDo);
                log.warn("删除当前文档出现错误，索引：{}和文档ID：{}不能为空！", index, id);
                return;
            }
            bulkRequest.add(new DeleteRequest(index, id));
        });

        if (async)
        {
            restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>()
            {
                @Override
                public void onResponse(BulkResponse bulkItemResponses)
                {
                    bulkItemResponses.forEach(bulkItemResponse ->
                    {
                        if (bulkItemResponse.status() != RestStatus.OK)
                        {
                            log.error("删除文档出现错误，索引：{}，文档ID：{}，错误消息：{}", bulkItemResponse.getIndex(), bulkItemResponse
                                    .getId(), bulkItemResponse.getFailureMessage());
                        }
                    });
                }

                @Override
                public void onFailure(Exception e)
                {
                    log.error("异步批量删除文档发生异常!");
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.BATCH_DELETE_DOCUMENT);
                }
            });
        }
        else
        {
            try
            {
                final BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                bulk.forEach(bulkItemResponse ->
                {
                    if (bulkItemResponse.isFailed())
                    {
                        log.error("删除文档失败，文档ID：{}，当前索引：{}，失败信息：{}", bulkItemResponse.getId(), bulkItemResponse
                                .getIndex(), bulkItemResponse.getFailureMessage());
                    }
                });
            }
            catch (Exception e)
            {
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.BATCH_DELETE_DOCUMENT);
            }

        }
        return true;
    }

    /**
     * [简要描述]:依据条件查询总数
     * [详细描述]:
     *
     * @param elasticSearchRequestDo : 查询条件
     * @return long
     **/
    @Override
    public <T> long count(ElasticSearchRequestDo<T> elasticSearchRequestDo)
    {
        if (Objects.isNull(elasticSearchRequestDo) || StringUtils.isBlank(elasticSearchRequestDo.getIndex()))
        {
            log.error("查询总数错误，查询请求参数为空!请求参数：{}", Objects.isNull(elasticSearchRequestDo) ?
                    "null" :
                    JSONObject.toJSONString(elasticSearchRequestDo));
            return 0;
        }
        CountRequest countRequest = new CountRequest(elasticSearchRequestDo.getIndex());
        countRequest.source(elasticSearchRequestDo.getSearchSourceBuilder());
        try
        {
            return restHighLevelClient.count(countRequest, RequestOptions.DEFAULT).getCount();
        }
        catch (Exception e)
        {
            log.error("查询总数出现异常，索引：{}，查询条件：{}", elasticSearchRequestDo.getIndex(), elasticSearchRequestDo
                    .getSearchSourceBuilder());
            SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.COUNT);
        }
        return 0;
    }

    /**
     * [简要描述]:字段聚合
     * [详细描述]:
     *
     * @param elasticSearchRequestDo :
     * @return java.util.List<java.lang.String>
     **/
    @Override
    public <T> List<String> aggregations(ElasticSearchRequestDo<T> elasticSearchRequestDo)
    {
        List<String> aggFields = new ArrayList<>();
        if (!Objects.isNull(elasticSearchRequestDo) && !Objects.isNull(elasticSearchRequestDo.getSearchSourceBuilder())
                && StringUtils.isNotBlank(elasticSearchRequestDo.getIndex()) && StringUtils
                .isNotBlank(elasticSearchRequestDo.getAggField()))
        {
            String index = elasticSearchRequestDo.getIndex();
            String field = elasticSearchRequestDo.getAggField();
            SearchSourceBuilder searchSourceBuilder = elasticSearchRequestDo.getSearchSourceBuilder();
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.source(searchSourceBuilder);
            try
            {
                final SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                Terms terms = search.getAggregations().get(field);
                if (null != terms)
                {
                    terms.getBuckets().forEach(bucket -> aggFields.add(bucket.getKey().toString()));
                }
                else
                {
                    log.error("聚合操作无返回结果,索引：{}，聚合字段：{}，聚合条件：{}", index, field, JSONObject
                            .toJSONString(searchSourceBuilder));
                }
            }
            catch (Exception e)
            {
                log.error("聚合出现失败，索引：{}，聚合字段：{}，聚合条件：{}", index, field, JSONObject
                        .toJSONString(searchSourceBuilder), e);
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.AGGREGATION);
            }
        }
        else
        {
            log.error("聚合出现错误，请求参数不合法，聚合请求参数：{}", Objects.isNull(elasticSearchRequestDo) ?
                    "null" :
                    JSONObject.toJSONString(elasticSearchRequestDo));
        }
        return aggFields;
    }

    /**
     * [简要描述]:查询结果处理
     * [详细描述]:
     *
     * @param hits : 返回数据
     * @param docCls：返回数据类型
     * @param highlightFieldList: 高亮字段
     */
    private <T> List<ElasticSearchResultDo<T>> processSearchResult(SearchHits hits, Class<T> docCls,
                                                                   List<String> highlightFieldList)
    {
        // 数据结果
        List<ElasticSearchResultDo<T>> searchResponses = new ArrayList<>();
        ElasticSearchResultDo<T> searchResultDo;
        String searchSource;
        // 迭代查询结果
        for (SearchHit searchHit : hits)
        {
            searchResultDo = new ElasticSearchResultDo<>();
            searchSource = searchHit.getSourceAsString();
            if (StringUtils.isNotBlank(searchSource))
            {
                searchResultDo.setDoc(JSONObject.parseObject(searchSource, docCls));
                // 高亮字段
                this.setHighlight(searchResultDo, searchHit, highlightFieldList);
                searchResponses.add(searchResultDo);
            }
        }
        return searchResponses;
    }

    /**
     * [简要描述]:设置高亮信息
     * [详细描述]:
     *
     * @param searchResultDo SearchResultDo
     * @param searchHit SearchHit
     * @param highlightFieldList 需要处理的高亮字段
     */
    private <T> void setHighlight(ElasticSearchResultDo<T> searchResultDo, SearchHit searchHit,
            List<String> highlightFieldList)
    {
        Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
        if (null != highlightFieldList)
        {
            Map<String, String> highlightMaps = new HashMap<>(highlightFieldList.size());
            highlightFieldList.forEach(highlightField ->
            {
                if (highlightFields.containsKey(highlightField))
                {
                    Text[] text = highlightFields.get(highlightField).getFragments();
                    if (text.length > 0)
                    {
                        highlightMaps.put(highlightField, text[0].string());
                    }
                }
            });
            searchResultDo.setHighlight(highlightMaps);
        }
    }

    /**
     * 获取keywords
     */
    @Override
    public List<String> getIkList(String keyword, String index, String field)
    {
        List<String> list = new ArrayList<>();
        try
        {
            AnalyzeRequest request = AnalyzeRequest.withField(index, field, keyword);
            AnalyzeResponse response = restHighLevelClient.indices().analyze(request, RequestOptions.DEFAULT);
            List<AnalyzeResponse.AnalyzeToken> tokens = response.getTokens();
            for (AnalyzeResponse.AnalyzeToken analyzeToken : tokens)
            {
                list.add(analyzeToken.getTerm());
            }
        }
        catch (IOException e)
        {
            log.info("获取keywords失败！");
        }
        return list;
    }
}
