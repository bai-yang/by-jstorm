package com.a.eye.by.jstorm.base;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class MyFunction extends BaseFunction {

    private static final long serialVersionUID = -9218338377828111050L;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        int a = tuple.getInteger(0);
        int b = tuple.getInteger(1);
        collector.emit(new Values(a + b, a * b));
    }

}
