package br.com.bicmsystems;

import br.com.bicmsystems.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class GenerateAllReportsServlet extends HttpServlet {

    private final KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<>();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try {

            batchDispatcher.send("ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                                     new CorrelationId(GenerateAllReportsServlet.class.getSimpleName()),
                                  "ECOMMERCE_USER_GENERATE_READING_REPORT",
                                 "ECOMMERCE_USER_GENERATE_READING_REPORT");

            System.out.println("Sent generate report to all users.");

            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Report requests generated.");

        } catch (ExecutionException | InterruptedException ex) {
            throw new ServletException(ex);
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        batchDispatcher.close();
    }

}
