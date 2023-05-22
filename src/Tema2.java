import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Tema2 {
    public static void main(String[] args) {
        String folder = args[0];
        int noOfThreads = Integer.parseInt(args[1]);
        AtomicInteger noOfOrders = new AtomicInteger(0);

        ConcurrentHashMap<String, Integer> orders = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, ArrayList<String>> products = new ConcurrentHashMap<>();
        List<String> ordersOut = Collections.synchronizedList(new ArrayList<>());
        List<String> productsOut = Collections.synchronizedList(new ArrayList<>());

        ExecutorService tpe = Executors.newFixedThreadPool(noOfThreads);

        tpe.submit(() -> {
            try {
                BufferedReader br = new BufferedReader(new FileReader(folder + "/orders.txt"));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] split = line.split(",");
                    orders.put(split[0], Integer.parseInt(split[1]));
                    noOfOrders.incrementAndGet();
                }
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        tpe.submit(() -> {
            try {
                BufferedReader br = new BufferedReader(new FileReader(folder + "/order_products.txt"));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] split = line.split(",");
                    if (!products.containsKey(split[0])) {
                        products.put(split[0], new ArrayList<>());
                    }
                    products.get(split[0]).add(split[1]);
                }
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        tpe.shutdown();
        try {
            //noinspection ResultOfMethodCallIgnored
            tpe.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        AtomicInteger inQueue = new AtomicInteger(0);
        ExecutorService tpeLv1 = Executors.newFixedThreadPool(noOfThreads);
        ExecutorService tpeLv2 = Executors.newFixedThreadPool(noOfThreads);

        int orderChunkSize = noOfOrders.get() / noOfThreads;
        for (int i = 0; i < noOfThreads; i++) {
            int orderIndex = i * orderChunkSize;
            if (i == noOfThreads - 1) {
                orderChunkSize = noOfOrders.get() - orderIndex;
            }
            tpeLv1.submit(new OrderProcessor(inQueue, tpeLv1, tpeLv2, orders, products, ordersOut, productsOut, orderIndex, orderChunkSize));
            inQueue.incrementAndGet();
        }

        try {
            //noinspection ResultOfMethodCallIgnored
            tpeLv1.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        tpe = Executors.newFixedThreadPool(noOfThreads);
        tpe.submit(() -> {
            try {
                BufferedWriter bw = new BufferedWriter(new FileWriter("orders_out.txt"));
                for (String order : ordersOut) {
                    bw.write(order);
                    bw.newLine();
                }
                bw.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        tpe.submit(() -> {
            try {
                BufferedWriter bw = new BufferedWriter(new FileWriter("order_products_out.txt"));
                for (String product : productsOut) {
                    bw.write(product);
                    bw.newLine();
                }
                bw.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        tpe.shutdown();
    }
}
