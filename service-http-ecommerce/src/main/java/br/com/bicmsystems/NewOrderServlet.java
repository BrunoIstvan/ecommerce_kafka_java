package br.com.bicmsystems;

import br.com.bicmsystems.dispatcher.KafkaDispatcher;
import br.com.bicmsystems.repository.OrdersDatabase;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try {

            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amout"));
            var orderId = req.getParameter("uuid");
            var order = new Order(orderId, amount, email);

            try(var database = new OrdersDatabase()) {

                if (database.saveNewOrder(order)) {

                    var id = new CorrelationId(NewOrderServlet.class.getSimpleName());
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", id, email, order);

                    System.out.println("New Order sent successfully.");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("New Order sent successfully.");

                } else {
                    System.out.println("Old Order received.");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("Old Order received.");
                }
            }
        } catch (SQLException | ExecutionException | InterruptedException ex) {
            throw new ServletException(ex);
        }
    }
}
