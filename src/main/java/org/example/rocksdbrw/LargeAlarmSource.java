package org.example.rocksdbrw;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class LargeAlarmSource implements SourceFunction<LargeAlarmEvent> {
    public volatile boolean running = true;
    public int sleepMs;
    public int maxId;
    public int maxKey;

    private final int minASCII = 64; // 97: 'a', 64: '@'
    private final int maxASCII = 122; // 122: 'z'

    private final int stringLength;
    private Random random;

    public LargeAlarmSource(int maxId, int maxKey, int len, int sleepMs) {
        this.maxId  = maxId;
        this.maxKey = maxKey;
        this.stringLength = len;
        this.random = new Random();
        this.sleepMs = sleepMs;
    }

    @Override
    public void run(SourceContext<LargeAlarmEvent> sourceContext) throws Exception {
        while (this.running){
            sourceContext.collect( new LargeAlarmEvent(
                    ""+ ( (int) (this.maxId * Math.random() ) ),
                    ""+ ( (int) (this.maxKey * Math.random() ) ),
                    getRandomString()
            ));

            System.out.println("I am sleeping " + this.sleepMs);
            Thread.sleep(this.sleepMs);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    public String getRandomString() {
        StringBuilder buffer = new StringBuilder(stringLength);
        for (int i = 0; i < stringLength; i++) {
            int randomLimitedInt = minASCII + (int) (random.nextFloat() * (maxASCII - minASCII + 1));
            buffer.append((char) randomLimitedInt);
        }

        return buffer.toString();
    }
}
