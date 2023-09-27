package br.com.bicmsystems;

public record User(String uuid) {

    @Override
    public String toString() {
        return "{ uuid: " + uuid() + "} ";
    }

}
