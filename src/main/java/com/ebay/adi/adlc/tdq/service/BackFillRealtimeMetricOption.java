package com.ebay.adi.adlc.tdq.service;

public class BackFillRealtimeMetricOption extends BaseOption {
    // yyyy-MM-dd HH:mm:ss
    private String date;

    @Override
    public String getDate() {
        return date;
    }

    @Override
    public void setDate(String date) {
        this.date = date;
    }
}
