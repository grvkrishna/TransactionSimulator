package org.grv.ingest.buffer;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;

public class BoundedBuffer<T> implements Buffer<T> {

    Queue<T> arrayDeque;

    private final int capacity;
    private final Object lock = new Object();
    public BoundedBuffer(int capacity) {
        this.arrayDeque = new ArrayDeque<>();
        this.capacity = capacity;
    }

    @Override
    public void put(T item) throws InterruptedException {
        Objects.requireNonNull(item);
        synchronized (lock){
           while (capacity == arrayDeque.size()){
               lock.wait();
           }
           arrayDeque.offer(item);
           lock.notifyAll();
       }
    }

    @Override
    public T take() throws InterruptedException {
        synchronized (lock) {
            while (arrayDeque.isEmpty()){
                lock.wait();
            }
            T item = arrayDeque.poll();
            lock.notifyAll();
            return item;
        }
    }

    @Override
    public int size() {
        synchronized (lock) {
            return arrayDeque.size();
        }
    }
}
