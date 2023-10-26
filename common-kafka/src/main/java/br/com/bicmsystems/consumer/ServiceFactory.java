package br.com.bicmsystems.consumer;

public interface ServiceFactory<T> {

    ConsumerService<T> create() throws Exception;

}
