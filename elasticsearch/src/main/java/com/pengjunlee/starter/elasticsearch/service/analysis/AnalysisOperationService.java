package com.pengjunlee.starter.elasticsearch.service.analysis;

import org.elasticsearch.common.settings.Settings;

/**
 * [简要描述]:
 * [详细描述]:
 *
 * @author mjye
 * @version 1.0, 2020/4/26 14:31
 * @since JDK 1.8
 */
public interface AnalysisOperationService
{
    boolean updateIndexAnalysis(String index, Settings.Builder put);
    boolean updateIndexAnalysis(String index, String put);
}
