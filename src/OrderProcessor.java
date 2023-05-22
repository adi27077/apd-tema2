import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class OrderProcessor implements Runnable {
    private final AtomicInteger inQueue;
    private final ExecutorService tpeLv1;
    private final ExecutorService tpeLv2;
    private final ConcurrentHashMap<String, Integer> orders;
    private final ConcurrentHashMap<String, ArrayList<String>> products;
    private final List<String> ordersOut;
    private final List<String> productsOut;
    private final int orderIndex;
    private final int orderChunkSize;

    public OrderProcessor(AtomicInteger inQueue, ExecutorService tpeLv1, ExecutorService tpeLv2, ConcurrentHashMap<String, Integer> orders,
                          ConcurrentHashMap<String, ArrayList<String>> products, List<String> ordersOut, List<String> productsOut,
                          int orderIndex, int orderChunkSize) {
        this.inQueue = inQueue;
        this.tpeLv1 = tpeLv1;
        this.tpeLv2 = tpeLv2;
        this.orders = orders;
        this.products = products;
        this.ordersOut = ordersOut;
        this.productsOut = productsOut;
        this.orderIndex = orderIndex;
        this.orderChunkSize = orderChunkSize;
    }

    @Override
    public void run() {
        for (int i = orderIndex; i < orderIndex + orderChunkSize; i++) {
            String orderId = (String) orders.keySet().toArray()[i];
            int noOfProducts = orders.get(orderId);
            if (noOfProducts > 0) {
                String orderOut = orderId + "," + noOfProducts;
                Future<Integer> productsShipped = tpeLv2.submit(new ProductProcessor(orderId, products, productsOut));
                try {
                    if (productsShipped.get() == noOfProducts) {
                        orderOut += ",shipped";
                    }
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                ordersOut.add(orderOut);
            }
        }

        int left = inQueue.decrementAndGet();
        if (left == 0) {
            tpeLv1.shutdown();
            tpeLv2.shutdown();
        }
    }

}
