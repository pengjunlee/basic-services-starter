package com.pengjunlee.starter.elasticsearch.domain;

import lombok.Data;

import java.util.List;

/**
 * [简要描述]:
 * [详细描述]:
 *
 * @author pengjunlee
 * @create 2020-12-02 15:32
 */
@Data
public class EsTaskDto
{
    /**
     * 需要查询的集群名称
     */
    private List<String> clusterName;

    /**
     * 需要查询的相关运行任务节点的名称
     */
    private List<String> nodeNames;

    /**
     * 父任务id
     * 由节点id和运行在该节点下的任务id组成
     */
    private String parentTaskId;

    /**
     * 是否需要得到任务的详情
     * 取消任务时不可用
     */
    private Boolean taskDetails;

    /**
     * 是否需要等待查询到的任务完成后返回结果
     * 取消任务时不可用
     */
    private Boolean waitForCompletion;

    /**
     * 如果等待任务完成后返回结果，等待的超时时间
     * 单位： 秒
     * 取消任务时不可用
     */
    private Long waitForTimeout;
}