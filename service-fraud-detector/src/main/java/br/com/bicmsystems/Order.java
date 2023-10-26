package br.com.bicmsystems;

import java.math.BigDecimal;

public record Order(String orderId, BigDecimal amount, String email) {


    @Override
    public String toString() {
        return "{ orderId: " + orderId() + " - " +
                 "email: " + email() + " - " +
                "amount: " + amount() + "} ";
    }
}
