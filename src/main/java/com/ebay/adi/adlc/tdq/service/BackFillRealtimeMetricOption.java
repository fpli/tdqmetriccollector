package com.ebay.adi.adlc.tdq.service;

public class BackFillRealtimeMetricOption extends BaseOption {
    // yyyy-MM-dd HH:mm:ss
    private String start;

    private String end;

    public String getStart() {
        return start;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }
}
