/*
 * Winner
 * 文件名  :SearchResponse.java
 * 创建人  :llxiao
 * 创建时间:2018年1月11日
 */

package com.pengjunlee.starter.elasticsearch.domain;

import lombok.Data;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Map;

/**
 * [简要描述]:搜索结果<br/>
 * [详细描述]:<br/>
 *
 * @author pengjunlee
 * @create 2020-12-02 15:32
 */
@Data
public class ElasticSearchResultDo<T>
{
    /**
     * 返回文档
     */
    private T doc;

    /**
     * 高亮字段 属性-高亮值
     */
    private Map<String, String> highlight;

    /**
     * [简要描述]:<br/>
     * [详细描述]:<br/>
     *
     * @return
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }

}
