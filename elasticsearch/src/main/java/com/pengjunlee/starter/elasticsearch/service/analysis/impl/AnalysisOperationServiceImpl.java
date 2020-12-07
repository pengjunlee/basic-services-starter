package com.pengjunlee.starter.elasticsearch.service.analysis.impl;

import com.pengjunlee.starter.elasticsearch.common.ElasticsearchOperationEnum;
import com.pengjunlee.starter.elasticsearch.common.SearchExceptionUtil;
import com.pengjunlee.starter.elasticsearch.service.analysis.AnalysisOperationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * [简要描述]:
 * [详细描述]:
 *
 * @author mjye
 * @version 1.0, 2020/4/26 14:32
 * @since JDK 1.8
 */
@Slf4j
@Service
public class AnalysisOperationServiceImpl implements AnalysisOperationService
{
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Override
    public boolean updateIndexAnalysis(String index, Settings.Builder put)
    {
        if (StringUtils.isBlank(index))
        {
            log.error("索引参数为空!");
            return false;
        }
        // ES不支持 大写驼峰，必须小写
        index = index.toLowerCase();

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index);
        updateSettingsRequest.settings(put);

        boolean result;
        try
        {
            AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices()
                    .putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
            result = acknowledgedResponse.isAcknowledged();
        }
        catch (IOException e)
        {
            result =false;
            SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.UPDATE_INDEX_ANALYSIS);
        }
        return result;
    }

    @Override
    public boolean updateIndexAnalysis(String index, String put)
    {
        if (StringUtils.isBlank(index))
        {
            log.error("索引参数为空!");
            return false;
        }
        // ES不支持 大写驼峰，必须小写
        index = index.toLowerCase();

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index);
        updateSettingsRequest.settings(put, XContentType.JSON);

        boolean result;
        try
        {
            AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices()
                    .putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
            result = acknowledgedResponse.isAcknowledged();
        }
        catch (IOException e)
        {
            result =false;
            SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.UPDATE_INDEX_ANALYSIS);
        }
        return result;
    }
}