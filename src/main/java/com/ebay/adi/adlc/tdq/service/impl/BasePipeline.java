package com.ebay.adi.adlc.tdq.service.impl;

import com.ebay.adi.adlc.tdq.service.BaseOption;
import com.ebay.adi.adlc.tdq.service.Pipeline;
import org.apache.commons.cli.DefaultParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasePipeline<T extends BaseOption> implements Pipeline<T> {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected DefaultParser getDefaultParser() {
        DefaultParser.Builder builder = DefaultParser.builder();
        return builder.build();
    }

    @Override
    public T parseCommand(String[] args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void process(BaseOption parameter) {
        throw new UnsupportedOperationException();
    }
}
