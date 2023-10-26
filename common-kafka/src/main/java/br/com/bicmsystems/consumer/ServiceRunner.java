package br.com.bicmsystems.consumer;

import java.util.concurrent.Executors;

public class ServiceRunner<T> {

    private final ServiceProvider<T> provider;

    public ServiceRunner(ServiceFactory<T> factory) {
        this.provider = new ServiceProvider<>(factory);
    }

    public void start(int threadCounts) {

        var pool = Executors.newFixedThreadPool(threadCounts);

        for (int i = 0; i < threadCounts; i++) {
            pool.submit(this.provider);
        }

    }


}
