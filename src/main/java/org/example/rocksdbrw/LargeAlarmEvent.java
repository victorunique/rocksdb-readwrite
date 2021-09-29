package org.example.rocksdbrw;

public class LargeAlarmEvent {
    public String id;
    public String key;
    public String value;

    public LargeAlarmEvent(String id, String key, String value) {
        this.id = id;
        this.key = key;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
