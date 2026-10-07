package org.grv.ingest.buffer;

public interface Buffer<T> {

    void put(T item) throws InterruptedException;
    T take() throws InterruptedException;
    int size();
    int capacity();
}
