package com.pengjunlee.starter.elasticsearch.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;

/**
 * [简要描述]: 搜索请求Do
 * [详细描述]:
 *
 * @author pengjunlee
 * @create 2020-12-02 15:32
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ElasticSearchRequestDo<T>
{
    /**
     * 搜索条件
     */
    private SearchSourceBuilder searchSourceBuilder;
    /**
     * 高亮字段
     */
    private List<String> highlightFieldList;
    /**
     * 数据对象
     */
    private Class<T> docCls;
    /**
     * 页数，默认1
     */
    private Integer pageNo = 1;

    /**
     * 也显示条数 10条
     */
    private Integer pageSize = 10;

    /**
     * 索引
     */
    private String index;

    /**
     * 是否使用分页 默认true
     */
    private boolean usePage = true;

    /**
     * 聚合字段
     */
    private String aggField;
}
