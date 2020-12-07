package com.pengjunlee.starter.elasticsearch.service.task.impl;

import com.pengjunlee.starter.elasticsearch.domain.EsTaskDto;
import com.pengjunlee.starter.elasticsearch.service.task.TaskService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.tasks.CancelTasksRequest;
import org.elasticsearch.client.tasks.CancelTasksResponse;
import org.elasticsearch.client.tasks.TaskId;
import org.elasticsearch.common.unit.TimeValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * [简要描述]:
 * [详细描述]:
 *
 * @author mjye
 * @version 1.0, 2020/1/16 09:40
 * @since JDK 1.8
 */
@Slf4j
@Service
public class TaskServiceImpl implements TaskService {
    @Autowired
    private RestHighLevelClient client;

    @Override
    public ListTasksResponse getEsTaskBySync(EsTaskDto esTaskDto) {
        ListTasksRequest request = new ListTasksRequest();
        if (null != esTaskDto) {
            if (CollectionUtils.isNotEmpty(esTaskDto.getClusterName())) {
                request.setActions(esTaskDto.getClusterName().toArray(new String[]{}));
            }

            if (CollectionUtils.isNotEmpty(esTaskDto.getNodeNames())) {
                request.setNodes(esTaskDto.getNodeNames().toArray(new String[]{}));
            }

            if (StringUtils.isNotEmpty(esTaskDto.getParentTaskId())) {
                request.setParentTaskId(new org.elasticsearch.tasks.TaskId("parentTaskId", Long.parseLong(esTaskDto.getParentTaskId())));
            }

            if (null != esTaskDto.getTaskDetails()) {
                request.setDetailed(esTaskDto.getTaskDetails());
            }

            if (null != esTaskDto.getWaitForCompletion()) {
                request.setWaitForCompletion(esTaskDto.getWaitForCompletion());
                if (null != esTaskDto.getWaitForTimeout()) {
                    request.setTimeout(TimeValue.timeValueMinutes(esTaskDto.getWaitForTimeout()));
                }

            }
        }
        try {
            return client.tasks().list(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("获取任务信息失败", e);
            throw new ElasticsearchException("获取任务失败");
        }
    }

    @Override
    public CancelTasksResponse cancelTsakBySync(EsTaskDto esTaskDto) {
        if (null == esTaskDto) {
            log.error("取消任务失败条件不能为空");
            throw new ElasticsearchException("取消任务失败，参数不能为空");
        }
        boolean isAllNull = true;
        CancelTasksRequest.Builder builder = new CancelTasksRequest.Builder();
        if (CollectionUtils.isNotEmpty(esTaskDto.getClusterName())) {
            builder.withActionsFiltered(esTaskDto.getClusterName());
            isAllNull = false;
        }

        if (CollectionUtils.isNotEmpty(esTaskDto.getNodeNames())) {
            builder.withNodesFiltered(esTaskDto.getNodeNames());
            isAllNull = false;
        }

        if (StringUtils.isNotEmpty(esTaskDto.getParentTaskId())) {
            builder.withParentTaskId(new TaskId(esTaskDto.getParentTaskId()));
            isAllNull = false;
        }
        CancelTasksRequest cancelTasksRequest = builder.build();
        if (isAllNull) {
            log.error("取消任务失败条件不能为空");
            throw new ElasticsearchException("取消任务失败，条件不能为空");
        } else {
            try {
                return client.tasks().cancel(cancelTasksRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("取消任务异常", e);
                throw new ElasticsearchException("取消任务异常【{}】", e.getMessage());
            }
        }
    }

}