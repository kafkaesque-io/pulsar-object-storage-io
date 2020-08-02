package com.kesque.pulsar.sink.s3.storage;

import java.io.IOException;

public class ConnectException extends RuntimeException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public ConnectException(String message) {
        super(message);
    }
    
    public ConnectException(Throwable message) {
        super(message);
    }
}