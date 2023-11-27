package com.ebay.adi.adlc.tdq.service.impl;

import com.ebay.adi.adlc.tdq.service.BaseOption;
import com.ebay.adi.adlc.tdq.service.Pipeline;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasePipeline<T extends BaseOption> implements Pipeline<T> {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected Parser getDefaultParser() {
        return new BasicParser();
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
