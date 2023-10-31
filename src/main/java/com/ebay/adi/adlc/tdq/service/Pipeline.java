package com.ebay.adi.adlc.tdq.service;

public interface Pipeline<T> {
    T parseCommand(String[] args);

    <U extends T> void process(U parameter);
}
