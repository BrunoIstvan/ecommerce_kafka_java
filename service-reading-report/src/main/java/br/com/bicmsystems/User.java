package br.com.bicmsystems;

import java.math.BigDecimal;

public record User(String uuid) {

    @Override
    public String toString() {
        return "{ uuid: " + uuid() + "} ";
    }

    public String getReportPath() {
        return "target/" + uuid + "-report.txt";
    }
}
