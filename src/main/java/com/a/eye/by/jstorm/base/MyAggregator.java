package com.a.eye.by.jstorm.base;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class MyAggregator implements CombinerAggregator<Integer> {

    private static final long serialVersionUID = -1607026423138054215L;

    @Override
    public Integer init(TridentTuple tuple) {
        return 1;
    }

    @Override
    public Integer combine(Integer val1, Integer val2) {
        return val1 + val2;
    }

    @Override
    public Integer zero() {
        return 1;
    }

}
