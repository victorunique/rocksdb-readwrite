package org.example.rocksdbrw;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

class MapStateProcessFunction extends KeyedProcessFunction<String, AlarmEvent, AlarmEvent> {
    private MapState<String, Long> mapState;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, Long> descriptorMapStateChanges = new MapStateDescriptor<>(
                "mapstate",
                TypeInformation.of(new TypeHint<String>() {}),
                TypeInformation.of(new TypeHint<Long>() {}));
        mapState = getRuntimeContext().getMapState(descriptorMapStateChanges);
    }

    @Override
    public void processElement(AlarmEvent event, Context context, Collector<AlarmEvent> collector) throws Exception {
        Long prevVal = mapState.get(event.getKey());
        mapState.put(event.getKey(), event.getValue());

        collector.collect(event);
    }
}

class ValueStateProcessFunction extends KeyedProcessFunction<String, AlarmEvent, AlarmEvent> {
    private ValueState<Map<String, Long>> valueState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Map<String, Long>> descriptor = new ValueStateDescriptor<>(
                "valstate",
                TypeInformation.of(new TypeHint<Map<String,Long>>() {}));
        valueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(AlarmEvent event, Context context, Collector<AlarmEvent> collector) throws Exception {
        Map<String,Long> prevVal = valueState.value();
        if (prevVal == null) {
            prevVal = new HashMap<>();
        }

        prevVal.put(event.getKey(), event.getValue());
        valueState.update(prevVal);

        collector.collect(event);
    }
}

class LargeMapStateProcessFunction extends KeyedProcessFunction<String, LargeAlarmEvent, LargeAlarmEvent> {
    private MapState<String, String> mapState;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, String> descriptorMapStateChanges = new MapStateDescriptor<>(
          "lmapstate",
          TypeInformation.of(new TypeHint<String>() {}),
          TypeInformation.of(new TypeHint<String>() {}));
        mapState = getRuntimeContext().getMapState(descriptorMapStateChanges);
    }

    @Override
    public void processElement(LargeAlarmEvent event, Context context, Collector<LargeAlarmEvent> collector) throws Exception {
        String prevVal = mapState.get(event.getKey());
        mapState.put(event.getKey(), event.getValue());

        collector.collect(event);
    }
}

class LargeValueStateProcessFunction extends KeyedProcessFunction<String, LargeAlarmEvent, LargeAlarmEvent> {
    private ValueState<Map<String, String>> valueState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Map<String, String>> descriptor = new ValueStateDescriptor<>(
                "lvalstate",
                TypeInformation.of(new TypeHint<Map<String,String>>() {}));
        valueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(LargeAlarmEvent event, Context context, Collector<LargeAlarmEvent> collector) throws Exception {
        Map<String,String> prevVal = valueState.value();
        if (prevVal == null) {
            prevVal = new HashMap<>();
        }

        prevVal.put(event.getKey(), event.getValue());
        valueState.update(prevVal);

        collector.collect(event);
    }
}


