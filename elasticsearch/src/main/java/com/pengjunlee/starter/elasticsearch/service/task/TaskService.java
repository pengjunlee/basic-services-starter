package com.pengjunlee.starter.elasticsearch.service.task;

import com.pengjunlee.starter.elasticsearch.domain.EsTaskDto;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.tasks.CancelTasksResponse;

/**
 * [简要描述]:
 * [详细描述]:
 *
 * @author mjye
 * @version 1.0, 2020/1/16 09:39
 * @since JDK 1.8
 */
public interface TaskService {
    /**
     * [简要描述]: 查询任务
     * [详细描述]:
     *
     * @param esTaskDto :
     * @return org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse
     * mjye  2020/1/16 - 15:35
     **/
    ListTasksResponse getEsTaskBySync(EsTaskDto esTaskDto);

    /**
     * [简要描述]: 取消任务
     * [详细描述]:
     *
     * @param esTaskDto :
     * @return org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse
     * mjye  2020/1/16 - 15:44
     **/
    CancelTasksResponse cancelTsakBySync(EsTaskDto esTaskDto);
}
