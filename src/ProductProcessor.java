import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class ProductProcessor implements Callable<Integer> {
    private final String orderId;
    private final ConcurrentHashMap<String, ArrayList<String>> products;
    private final List<String> productsOut;

    public ProductProcessor(String orderId, ConcurrentHashMap<String, ArrayList<String>> products, List<String> productsOut) {
        this.orderId = orderId;
        this.products = products;
        this.productsOut = productsOut;
    }

    @Override
    public Integer call() {
        int shipped = 0;
        for (String productId : products.get(orderId)) {
            if (productId != null) {
                productsOut.add(orderId + "," + productId + ",shipped");
                shipped++;
            }
        }

        return shipped;
    }
}
