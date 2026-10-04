package org.grv.service;

import org.grv.service.authpipeline.FraudCheck;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

public class CallableToSupplierAdaptor {

    public static <T> Supplier<T> adapt(Callable<T> callable){
       return  () -> {
            try {
                return callable.call();
            }  catch (RuntimeException e) {
                throw e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
       };
    }
}
