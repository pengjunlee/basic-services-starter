/*
 * Winner
 * 文件名  :PaginationDo.java
 * 创建人  :llxiao
 * 创建时间:2018年1月12日
 */

package com.pengjunlee.starter.elasticsearch.domain;

import lombok.Data;

import java.util.List;

/**
 * [简要描述]:查询结果实体类<br/>
 * [详细描述]:<br/>
 *
 * @author pengjunlee
 * @create 2020-12-02 15:32
 */
@Data
public class PaginationDo<ElasticSearchResultDo>
{
    /**
     * 查询数据结果
     */
    private List<ElasticSearchResultDo> results;

    /**
     * 总数
     */
    private int total;

    /**
     * 页显示数量
     */
    private int pageSize;

    /**
     * 当前页数
     */
    private int pageNo;

    /**
     * 查询所用时间(单位:毫秒)
     */
    private long took;
}
