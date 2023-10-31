package com.ebay.adi.adlc.tdq.service.impl;

import com.ebay.adi.adlc.tdq.service.PageProfilingOption;

public class PageProfilingPipeline extends BasePipeline<PageProfilingOption> {

    @Override
    public PageProfilingOption parseCommand(String[] args) {
        super.parseCommand(args);
        // todo parse command args to build java pojo
        return new PageProfilingOption();
    }

    @Override
    public void process(PageProfilingOption parameter) {
        super.process(parameter);
    }
}
