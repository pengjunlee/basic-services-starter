package com.pengjunlee.starter.elasticsearch.config;

import lombok.Data;

/**
 * [简要描述]:
 * [详细描述]:
 *
 * @author pengjunlee
 * @create 2020-12-02 15:32
 */
@Data
public class HostInfo {
    private String hostname;
    private int port;
    private String schema;

}
