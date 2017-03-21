package com.a.eye.by.jstorm.base;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class MyFilter extends BaseFilter {
    
    private static final long serialVersionUID = -3951195923966039092L;

    @Override
    public boolean isKeep(TridentTuple tuple) {
        return tuple.getInteger(0).intValue() < 10;
    }

}
