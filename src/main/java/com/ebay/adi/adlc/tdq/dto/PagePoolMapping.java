package com.ebay.adi.adlc.tdq.dto;

import java.io.Serializable;


public class PagePoolMapping implements Serializable {
    private Integer page_id;

    private long traffic;
    private String pool_name;

    public PagePoolMapping() {
    }

    public Integer getPageId() {
        return page_id;
    }

    public void setPageId(Integer pageId) {
        this.page_id = pageId;
    }

    public long getTraffic() {
        return traffic;
    }

    public void setTraffic(long traffic) {
        this.traffic = traffic;
    }

    public String getPoolName() {
        return pool_name;
    }

    public void setPoolName(String poolName) {
        this.pool_name = poolName;
    }

}
