package br.com.bicmsystems.dispatcher;

import br.com.bicmsystems.Message;
import br.com.bicmsystems.MessageAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;

public class GsonSerializer<T> implements Serializer<T> {

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(Message.class, new MessageAdapter<Message>())
            .create();

    @Override
    public byte[] serialize(String s, T t) {
        return gson.toJson(t).getBytes(Charset.defaultCharset());
    }
}
