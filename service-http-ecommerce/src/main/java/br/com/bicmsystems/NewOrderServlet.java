package br.com.bicmsystems;

import br.com.bicmsystems.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
        emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try {

            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amout"));
            var orderId = UUID.randomUUID().toString();
            var order = new Order(orderId, amount, email);
            var id = new CorrelationId(NewOrderServlet.class.getSimpleName());

            orderDispatcher.send("ECOMMERCE_NEW_ORDER", id, email, order);

            var emailText = new Email("Reporting status",
                    "Olá " + email + ", Thank you for your order! We are processing your order!");
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", id, email, emailText);

            System.out.println("New Order sent successfully.");

            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New Order sent successfully.");

        } catch (ExecutionException | InterruptedException ex) {
            throw new ServletException(ex);
        }
    }
}
