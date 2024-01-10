package com.ebay.adi.adlc.tdq.dto;

import java.io.Serializable;


public class PagePoolMapping implements Serializable {
    private Integer page_id;
    private String pool_name;

    private String dt;

    public PagePoolMapping(Integer page_id, String pool_name, String dt) {
        this.page_id = page_id;
        this.pool_name = pool_name;
        this.dt = dt;
    }

    public PagePoolMapping() {
    }

    public Integer getPageId() {
        return page_id;
    }

    public void setPageId(Integer pageId) {
        this.page_id = pageId;
    }

    public String getPoolName() {
        return pool_name;
    }

    public void setPoolName(String poolName) {
        this.pool_name = poolName;
    }
}
