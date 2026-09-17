package com.mediagw.codec;

import java.util.ArrayList;
import java.util.List;

/** 记录回调的测试监听器。 */
final class RecordingListener implements VideoDepacketizer.Listener {

    final List<ParamSets> paramSets = new ArrayList<>();
    final List<AccessUnit> accessUnits = new ArrayList<>();
    final List<String> errors = new ArrayList<>();

    @Override
    public void onParameterSets(ParamSets ps) {
        paramSets.add(ps);
    }

    @Override
    public void onAccessUnit(AccessUnit au) {
        accessUnits.add(au);
    }

    @Override
    public void onError(String message) {
        errors.add(message);
    }
}
