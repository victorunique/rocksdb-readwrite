package org.example.rocksdbrw;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class AlarmSource implements SourceFunction<AlarmEvent> {
    public volatile boolean running = true;
    public transient int sleepMs;
    public int maxId;
    public int maxKey;

    public AlarmSource(int maxId, int maxKey, int sleepMs) {
        this.maxId  = maxId;
        this.maxKey = maxKey;
        this.sleepMs = sleepMs;
    }

    @Override
    public void run(SourceContext<AlarmEvent> sourceContext) throws Exception {
        while (this.running){
            sourceContext.collect( new AlarmEvent(
                    ""+ ( (int) (this.maxId * Math.random() ) ),
                    ""+ ( (int) (this.maxKey * Math.random() ) ),
                    (long) (Long.MAX_VALUE * Math.random())
            ));
            Thread.sleep(sleepMs);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

}
