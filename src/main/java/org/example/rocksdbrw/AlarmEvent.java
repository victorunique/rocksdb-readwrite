package org.example.rocksdbrw;

public class AlarmEvent {
    public String id;
    public String key;
    public Long value;

    public AlarmEvent(String id, String key, Long value) {
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

    public Long getValue() {
        return value;
    }
}