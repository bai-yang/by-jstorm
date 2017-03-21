package com.a.eye.by.jstorm.base;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class MyReduceAggregator implements ReducerAggregator<Integer> {

    private static final long serialVersionUID = -4599801893387282501L;

    @Override
    public Integer init() {
        return 1;
    }

    @Override
    public Integer reduce(Integer curr, TridentTuple tuple) {
        return curr + tuple.getInteger(0);
    }

}
