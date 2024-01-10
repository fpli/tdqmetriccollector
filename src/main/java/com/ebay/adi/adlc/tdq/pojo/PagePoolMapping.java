package com.ebay.adi.adlc.tdq.pojo;

import java.io.Serializable;


public class PagePoolMapping implements Serializable {
    private Long pageId;
    private String poolName;

    public PagePoolMapping(Long pageId, String poolName) {
        this.pageId = pageId;
        this.poolName = poolName;
    }

    public PagePoolMapping() {
    }

    public Long getPageId() {
        return pageId;
    }

    public void setPageId(Long pageId) {
        this.pageId = pageId;
    }

    public String getPoolName() {
        return poolName;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }
}
