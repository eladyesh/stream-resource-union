package com.example.demo.model;

public class Span {
    private String traceId;
    private String span;
    private String service;

    public Span() {}

    public Span(String traceId, String span, String service) {
        this.traceId = traceId;
        this.span = span;
        this.service = service;
    }

    // Getters and setters
    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getSpan() {
        return span;
    }

    public void setSpan(String span) {
        this.span = span;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    @Override
    public String toString() {
        return "Span{" +
                "traceId='" + traceId + '\'' +
                ", span='" + span + '\'' +
                ", service='" + service + '\'' +
                '}';
    }
}
