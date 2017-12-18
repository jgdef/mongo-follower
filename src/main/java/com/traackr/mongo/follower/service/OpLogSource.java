package com.traackr.mongo.follower.service;

import java.io.Serializable;

public interface OpLogSource {
    Serializable take();
}
