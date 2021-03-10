package io.github.streamingwithflink.conversion.model;

import java.util.Map;
import java.util.Objects;

public class Event {

    private String app;
    private String channel;
    private String deviceId;
    private String sessionId;
    private String eventId;
    private Long eventTime;
    private String eventName;
    private Map<String, String> params;

    public Event() {
    }

    public Event(String app, String channel, String deviceId, String sessionId, String eventId, Long eventTime, String eventName) {
        this.app = app;
        this.channel = channel;
        this.deviceId = deviceId;
        this.sessionId = sessionId;
        this.eventId = eventId;
        this.eventTime = eventTime;
        this.eventName = eventName;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Event)) return false;
        Event event = (Event) o;
        return getApp().equals(event.getApp()) && getChannel().equals(event.getChannel())
                && getDeviceId().equals(event.getDeviceId()) && getSessionId().equals(event.getSessionId())
                && getEventId().equals(event.getEventId()) && getEventTime().equals(event.getEventTime())
                && getEventName().equals(event.getEventName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getApp(), getChannel(), getDeviceId(), getSessionId(), getEventId(), getEventTime(), getEventName(), getParams());
    }
}
