package com.traackr.mongo.follower.service;

import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class InMemoryOpLogSinkAndSource implements OpLogSinkAndSource {
    private static final Logger logger = Logger.getLogger(InMemoryOpLogSinkAndSource.class.getName());
    private final BlockingQueue<Serializable> queue = new ArrayBlockingQueue<>(4000);

    /** TODO: should deal with more useful types that Serializable */
    @Override
    public boolean send(Serializable data) {
        try {
            queue.put(data);
            return true;
        } catch (InterruptedException e) {
            logger.warning("Interrupted when sending data to oplog sink");
            e.printStackTrace(System.err);
        }

        return false;
    }

    @Override
    public boolean send(Serializable data, long timeout, TimeUnit timeUnit) {
        try {
            queue.offer(data, timeout, timeUnit);
            return true;
        } catch (InterruptedException e) {
            logger.warning("Interrupted when sending data to oplog sink with timeout");
            e.printStackTrace(System.err);
        }

        return false;
    }

    @Override
    public Serializable take() {
        Serializable ret = null;
        while (true) {
            try {
                ret = queue.take();
                break;
            } catch (InterruptedException e) {
                logger.warning("Interrupted when taking data from oplog source");
                e.printStackTrace(System.err);
            }
        }

        return ret;
    }

}
