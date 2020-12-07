package com.pengjunlee.starter.elasticsearch.service.index;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.Set;

/**
 * [简要描述]:
 * [详细描述]:
 *
 * @author pengjunlee
 * @create 2020-12-02 15:32
 */
public interface IndexOperationService {
    /**
     * [简要描述]:创建索引
     * [详细描述]:settings为空，取默认值{@link com.pengjunlee.starter.elasticsearch.common.ElasticsearchConstants#DEFAULT_SETTINGS}
     *
     * @param index    : 索引名称
     * @param settings : setting配置，默认
     * @param mappings : mapping设置
     * @return boolean
     **/
    boolean createIndex(String index, Settings.Builder settings, XContentBuilder mappings);

    /**
     * [简要描述]: 创建索引
     * [详细描述]:
     *
     * @param index    :
     * @param settings :
     * @return boolean
     **/
    boolean createIndex(String index, Settings.Builder settings);

    /**
     * [简要描述]: 创建索引包含别名
     * [详细描述]: 如果不传别名将由索引拼接别名后缀
     *
     * @param index      : 索引
     * @param aliasIndex : 别名
     * @param settings   : 索引设置
     * @param mappings   : 索引mapping
     * @return boolean
     **/
    boolean createIndex(String index, String aliasIndex, Settings.Builder settings, XContentBuilder mappings);

    /**
     * [简要描述]: 创建索引包含别名
     * [详细描述]: 如果不传别名将由索引拼接别名后缀
     *
     * @param index      : 索引
     * @param aliasIndex : 别名
     * @param settings   : 索引设置
     * @param mappings   : 索引mapping
     * @return boolean
     **/
    boolean createIndex(String index, String aliasIndex, XContentBuilder settings, XContentBuilder mappings);

    /**
     * [简要描述]:删除索引
     * [详细描述]:
     *
     * @param index  :
     * @param async: 是否异步执行，默认false
     * @return boolean
     **/
    boolean deleteIndex(String index, boolean async);

    /**
     * [简要描述]:索引是否存在
     * [详细描述]:
     *
     * @param index :
     * @return boolean
     **/
    boolean existIndex(String index);

    /**
     * [简要描述]: 重新索引
     * [详细描述]:
     *
     * @param targetIndex :
     * @param destIndex   :
     * @return boolean
     **/
    void asyncReindex(String[] targetIndex, String destIndex);

    /**
     * [简要描述]: 创建reindex任务
     * [详细描述]:
     *
     * @param targetIndex :
     * @param destIndex   :
     * @return void
     **/
    String createTaskReindex(String[] targetIndex, String destIndex);

    /**
     * [简要描述]:设置索引别名
     * [详细描述]:
     *
     * @param index :
     * @param alias :
     * @return void
     **/
    boolean setIndexAliases(String index, String alias);

    /**
     * [简要描述]:获取索引别名
     * [详细描述]:
     *
     * @param index :
     * @return void
     **/
    Set<AliasMetadata> getIndexAliases(String index);

    /**
     * [简要描述]:删除索引别名
     * [详细描述]:
     *
     * @param index :
     * @param alias :
     * @return boolean
     **/
    boolean removeIndexAliases(String index, String alias);

    /**
     * [简要描述]: 关闭索引
     * [详细描述]:
     *
     * @param index :
     * @return boolean
     **/
    boolean closeIndex(String index);


    /**
     * [简要描述]: 关闭索引
     * [详细描述]:
     *
     * @param index :
     * @return boolean
     **/
    boolean openIndex(String index);

    /**
     * [简要描述]: 设置索引mapping
     * [详细描述]:
     *
     * @param index   :
     * @param mapping :
     * @return boolean
     **/
    boolean setIndexMapping(String index, XContentBuilder mapping);
}
