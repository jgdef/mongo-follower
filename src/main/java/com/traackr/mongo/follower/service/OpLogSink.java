package com.traackr.mongo.follower.service;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public interface OpLogSink {
    boolean send(Serializable data);
    boolean send(Serializable data, long timeout, TimeUnit unit);
}
