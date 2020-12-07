package com.pengjunlee.starter.elasticsearch.service.index.impl;

import com.alibaba.fastjson.JSON;
import com.pengjunlee.starter.elasticsearch.common.ElasticsearchConstants;
import com.pengjunlee.starter.elasticsearch.common.ElasticsearchOperationEnum;
import com.pengjunlee.starter.elasticsearch.common.SearchExceptionUtil;
import com.pengjunlee.starter.elasticsearch.service.index.IndexOperationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.client.tasks.TaskSubmissionResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * [简要描述]: 索引相关操作类
 * [详细描述]:
 *
 * @author pengjunlee
 * @create 2020-12-02 15:32
 */
@Slf4j
@Service
public class IndexOperationServiceImpl implements IndexOperationService, ElasticsearchConstants {
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * [简要描述]:创建索引
     * [详细描述]:settings为空，取默认值{@link com.pengjunlee.starter.elasticsearch.common.ElasticsearchConstants#DEFAULT_SETTINGS}
     *
     * @param index    : 索引名称
     * @param settings : 索引配置
     * @param mappings : mapping设置
     * @return boolean
     **/
    @Override
    public boolean createIndex(String index, Settings.Builder settings, XContentBuilder mappings) {
        return this.disposeCreateIndex(index, null, settings, mappings);
    }

    @Override
    public boolean createIndex(String index, Settings.Builder settings) {
        return this.disposeCreateIndex(index, null, settings, null);
    }

    /**
     * [简要描述]:根据别名创建索引
     * [详细描述]:settings为空，取默认值{@link com.pengjunlee.starter.elasticsearch.common.ElasticsearchConstants#DEFAULT_SETTINGS}
     *
     * @param index      : 索引名称
     * @param aliasIndex 索引别名
     * @param settings   : 索引配置
     * @param mappings   : mapping设置
     * @return boolean
     **/
    @Override
    public boolean createIndex(String index, String aliasIndex, Settings.Builder settings, XContentBuilder mappings) {
        return this.disposeCreateIndex(index, aliasIndex, settings, mappings);
    }

    @Override
    public boolean createIndex(String index, String aliasIndex, XContentBuilder settings, XContentBuilder mappings) {
        return this.disposeCreateIndexSetting(index, aliasIndex, settings, mappings);
    }

    /**
     * [简要描述]:删除索引
     * [详细描述]:
     *
     * @param index                :
     * @param async:是否异步执行，默认false
     * @return boolean
     **/
    @Override
    public boolean deleteIndex(String index, boolean async) {
        if (indexPreHandle(index, ElasticsearchOperationEnum.DELETE_INDEX) == false) {
            return false;
        }
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        if (async) {
            final String tempIndex = index;
            // 异步
            restHighLevelClient.indices()
                    .deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<AcknowledgedResponse>() {
                        @Override
                        public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                            if (!acknowledgedResponse.isAcknowledged()) {
                                log.info("删除索引：{}失败", tempIndex);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.DELETE_INDEX);
                        }
                    });
        } else {
            try {
                AcknowledgedResponse deleteIndexResponse = restHighLevelClient.indices()
                        .delete(request, RequestOptions.DEFAULT);
                return deleteIndexResponse.isAcknowledged();
            } catch (Exception e) {
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.DELETE_INDEX);
            }

        }
        return false;
    }

    /**
     * [简要描述]:索引是否存在
     * [详细描述]:
     *
     * @param index :
     * @return boolean
     **/
    @Override
    public boolean existIndex(String index) {
        if (indexPreHandle(index, ElasticsearchOperationEnum.EXIST_INDEX) == false) {
            return false;
        }
        GetIndexRequest request = new GetIndexRequest(index);
        try {
            return restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.EXIST_INDEX);
        }
        return false;
    }

    private void reindexPreHandle(String[] targetIndex, String destIndex) {
        if (StringUtils.isBlank(destIndex) || targetIndex == null || targetIndex.length == 0) {
            log.error("重新索引失败，索引参数为空!");
            throw new ElasticsearchException("索引参数为空");
        }

        if (!this.existIndex(destIndex)) {
            log.error("重新索引失败，目标索引{}不存在!", destIndex);
            throw new ElasticsearchException("目标索引{}不存在", destIndex);
        }

        for (String target : targetIndex) {
            if (!this.existIndex(target)) {
                log.error("重新索引失败，源索引{}不存在!", target);
                throw new ElasticsearchException("源索引{}不存在!", target);
            }
        }

        // ES不支持 大写驼峰，必须小写
        destIndex = destIndex.toLowerCase();
    }

    @Override
    public void asyncReindex(String[] targetIndex, String destIndex) {

        reindexPreHandle(targetIndex, destIndex);

        ReindexRequest reindexRequest = new ReindexRequest(); //创建ReindexRequest
        reindexRequest.setSourceIndices(targetIndex); //添加要从源中复制的列表
        reindexRequest.setDestIndex(destIndex);  //添加目标索引
        String finalDestIndex = destIndex;
        restHighLevelClient
                .reindexAsync(reindexRequest, RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                        disposeReindex(response, targetIndex, finalDestIndex);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.REINDEX);
                    }
                });
    }

    @Override
    public String createTaskReindex(String[] targetIndex, String destIndex) {
        reindexPreHandle(targetIndex, destIndex);

        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(targetIndex);
        reindexRequest.setDestIndex(destIndex);
        reindexRequest.setRefresh(true);

        TaskSubmissionResponse reindexSubmission;
        try {
            reindexSubmission = restHighLevelClient.submitReindexTask(reindexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.REINDEX);
            throw new ElasticsearchException("创建reindex任务失败");
        }

        return reindexSubmission.getTask();
    }

    @Override
    public boolean setIndexAliases(String index, String alias) {
        if (StringUtils.isBlank(index)) {
            log.error("设置索引别名失败，索引参数为空!");
        }

        boolean result = false;
        if (this.existIndex(index)) {
            if (StringUtils.isBlank(alias)) {
                alias = index;
            }
            try {
                IndicesAliasesRequest request = new IndicesAliasesRequest();
                IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                        .index(index).alias(alias);
                request.addAliasAction(aliasAction);
                AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices()
                        .updateAliases(request, RequestOptions.DEFAULT);
                if (acknowledgedResponse.isAcknowledged()) {
                    result = true;
                    log.info("设置索引别名成功!");
                }

            } catch (Exception e) {
                log.error("设置索引别名失败!");
            }

        } else {
            log.error("索引：{}不存在!", index);
        }

        return result;
    }

    @Override
    public Set<AliasMetadata> getIndexAliases(String index) {
        if (StringUtils.isBlank(index)) {
            log.error("获取索引别名失败，索引参数为空!");
        }

        Set<AliasMetadata> aliases = null;
        if (!this.existIndex(index)) {
            log.error("索引{}不存在!", index);
            return null;
        }

        try {
            GetAliasesRequest request = new GetAliasesRequest();
            request.indices(index);
            //集群查找
            request.local(true);
            GetAliasesResponse getAliasesResponse = restHighLevelClient.indices()
                    .getAlias(request, RequestOptions.DEFAULT);
            if (getAliasesResponse.status().getStatus() == ElasticsearchConstants.HTTP_200) {
                aliases = getAliasesResponse.getAliases().get(index);
            }

        } catch (IOException e) {
            log.error("获取索引别名失败!");
        }

        return aliases;
    }

    @Override
    public boolean removeIndexAliases(String index, String alias) {
        if (StringUtils.isBlank(index) || StringUtils.isBlank(alias)) {
            log.error("删除索引别名失败，索引参数为空!");
        }

        boolean result = false;
        if (this.existIndex(index)) {
            try {
                IndicesAliasesRequest request = new IndicesAliasesRequest();
                IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                        .index(index).alias(alias);
                request.addAliasAction(aliasAction);
                AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices()
                        .updateAliases(request, RequestOptions.DEFAULT);
                if (acknowledgedResponse.isAcknowledged()) {
                    result = true;
                    log.info("删除索引{}别名{}成功!", index, alias);
                }

            } catch (Exception e) {
                log.error("删除索引别名失败!");
            }

        } else {
            log.error("索引：{}不存在!", index);
        }

        return result;
    }

    @Override
    public boolean closeIndex(String index) {
        if (indexPreHandle(index, ElasticsearchOperationEnum.CLOSE_INDEX) == false) {
            return false;
        }
        CloseIndexRequest request = new CloseIndexRequest(index);

        boolean result;
        try {
            CloseIndexResponse close = restHighLevelClient.indices().close(request, RequestOptions.DEFAULT);
            result = close.isAcknowledged();
        } catch (Exception e) {
            result = false;
            SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.CLOSE_INDEX);
        }
        return result;
    }

    @Override
    public boolean openIndex(String index) {
        if (indexPreHandle(index, ElasticsearchOperationEnum.OPEN_INDEX) == false) {
            return false;
        }
        OpenIndexRequest request = new OpenIndexRequest(index);

        boolean result;
        try {
            OpenIndexResponse close = restHighLevelClient.indices().open(request, RequestOptions.DEFAULT);
            result = close.isAcknowledged();
        } catch (Exception e) {
            result = false;
            SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.OPEN_INDEX);
        }
        return result;
    }

    @Override
    public boolean setIndexMapping(String index, XContentBuilder mapping) {
        if (indexPreHandle(index, ElasticsearchOperationEnum.PUT_MAPPING) == false) {
            return false;
        }
        PutMappingRequest request = new PutMappingRequest(index);
        request.source(mapping);
        boolean result;
        try {
            AcknowledgedResponse putMapping = restHighLevelClient.indices().putMapping(request, RequestOptions.DEFAULT);
            result = putMapping.isAcknowledged();
        } catch (Exception e) {
            result = false;
            SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.PUT_MAPPING);
        }
        return result;
    }

    private boolean indexPreHandle(String index, ElasticsearchOperationEnum operation) {
        if (StringUtils.isBlank(index)) {
            log.error("{}失败，索引参数为空!", operation.getOperationName());
            return false;
        }
        // ES不支持 大写驼峰，必须小写
        index = index.toLowerCase();
        return true;
    }

    private boolean disposeCreateIndex(String index, String aliasIndex, Settings.Builder settings,
                                       XContentBuilder mappings) {
        if (indexPreHandle(index, ElasticsearchOperationEnum.CREATE_INDEX) == false) {
            return false;
        }

        if (this.existIndex(index)) {
            // 如果存在先删除
            deleteIndex(index, false);
        }

        if (null == settings) {
            log.warn("使用默认setting配置创建索引：{}", index);
            settings = ElasticsearchConstants.DEFAULT_SETTINGS;
        }

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        createIndexRequest.settings(settings);
        return this.disposeCreateIndex(index, aliasIndex, createIndexRequest, mappings);
    }

    private boolean disposeCreateIndexSetting(String index, String aliasIndex, XContentBuilder settings,
                                              XContentBuilder mappings) {
        if (indexPreHandle(index, ElasticsearchOperationEnum.CREATE_INDEX) == false) {
            return false;
        }

        if (this.existIndex(index)) {
            // 如果存在先删除
            deleteIndex(index, false);
        }

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        createIndexRequest.settings(settings);
        return this.disposeCreateIndex(index, aliasIndex, createIndexRequest, mappings);
    }

    private boolean disposeCreateIndex(String index, String aliasIndex, CreateIndexRequest createIndexRequest,
                                       XContentBuilder mappings) {
        boolean result = false;

        if (StringUtils.isEmpty(aliasIndex)) {
            aliasIndex = index + ElasticsearchConstants.ALIAS_SUFFIX;
        }
        createIndexRequest.alias(new Alias(aliasIndex));

        if (null != mappings) {
            createIndexRequest.mapping(mappings);
        }
        try {
            // 同步创建
            final CreateIndexResponse createIndexResponse = restHighLevelClient.indices()
                    .create(createIndexRequest, RequestOptions.DEFAULT);

            // 所有节点是否已确认请求
            if (createIndexResponse.isAcknowledged()) {
                log.info("创建索引：{}所有节点已确认接收请求!", index);
            }
            // 是否在超时前为索引中的每个碎片启动了所需数量的分片副本
            if (createIndexResponse.isShardsAcknowledged()) {
                log.info("创建索引：{}所有节点分片副本都已创建成功!", index);
            }
            result = true;
        } catch (Exception e) {
            SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.CREATE_INDEX);
        }
        return result;
    }

    private void disposeReindex(BulkByScrollResponse bulkResponse, String[] targetIndex, String destIndex) {
        TimeValue timeTaken = bulkResponse.getTook();
        log.info("索引【{}】reindex到索引【{}】上总耗时：【{}】", Arrays.toString(targetIndex), destIndex, timeTaken.seconds());
        boolean timedOut = bulkResponse.isTimedOut();
        log.info("索引【{}】reindex到索引【{}】是否超时：【{}】", Arrays.toString(targetIndex), destIndex, timedOut);
        long totalDocs = bulkResponse.getTotal();
        log.info("索引【{}】reindex到索引【{}】处理的文档总数：【{}】", Arrays.toString(targetIndex), destIndex, totalDocs);
        long updatedDocs = bulkResponse.getUpdated();
        log.info("索引【{}】reindex到索引【{}】更新文档的数量：【{}】", Arrays.toString(targetIndex), destIndex, updatedDocs);
        long createdDocs = bulkResponse.getCreated();
        log.info("索引【{}】reindex到索引【{}】创建文档的数量：【{}】", Arrays.toString(targetIndex), destIndex, createdDocs);
        long deletedDocs = bulkResponse.getDeleted();
        log.info("索引【{}】reindex到索引【{}】删除文档的数量：【{}】", Arrays.toString(targetIndex), destIndex, deletedDocs);
        long batches = bulkResponse.getBatches();
        log.info("索引【{}】reindex到索引【{}】执行的次数：【{}】", Arrays.toString(targetIndex), destIndex, batches);
        long noops = bulkResponse.getNoops();
        log.info("索引【{}】reindex到索引【{}】未处理（跳过）的文档数：【{}】", Arrays.toString(targetIndex), destIndex, noops);
        long versionConflicts = bulkResponse.getVersionConflicts();
        log.info("索引【{}】reindex到索引【{}】存在版本冲突的文档数：【{}】", Arrays.toString(targetIndex), destIndex, versionConflicts);
        long bulkRetries = bulkResponse.getBulkRetries();
        log.info("索引【{}】reindex到索引【{}】请求操作的重试次数：【{}】", Arrays.toString(targetIndex), destIndex, bulkRetries);
        long searchRetries = bulkResponse.getSearchRetries();
        log.info("索引【{}】reindex到索引【{}】请求搜索超时的次数：【{}】", Arrays.toString(targetIndex), destIndex, searchRetries);
        List<ScrollableHitSource.SearchFailure> searchFailures = bulkResponse.getSearchFailures();
        log.info("索引【{}】reindex到索引【{}】在搜索阶段出现的问题：【{}】", Arrays.toString(targetIndex), destIndex, JSON
                .toJSONString(searchFailures));
        List<BulkItemResponse.Failure> bulkFailures = bulkResponse.getBulkFailures();
        log.info("索引【{}】reindex到索引【{}】在index阶段出现的问题：【{}】", Arrays.toString(targetIndex), destIndex, JSON
                .toJSONString(bulkFailures));
    }
}