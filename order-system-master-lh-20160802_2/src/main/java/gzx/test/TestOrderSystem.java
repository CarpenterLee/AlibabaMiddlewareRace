package gzx.test;

import com.alibaba.middleware.race.OrderSystemImpl;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.OrderSystem.*;
import gzx.test.QueryAndResult;

/**
 * Created by gzx on 16-7-13.
 */
public class TestOrderSystem {

    Boolean debugTesting = true;
    ArrayList<String> orderFiles;
    ArrayList<String> goodFiles;
    ArrayList<String> buyerFiles;
    ArrayList<String> storeFolders;


//    LinkedBlockingQueue<QueryAndResult> QueryQueue = new LinkedBlockingQueue<QueryAndResult>();

    ArrayBlockingQueue<QueryAndResult> QueryQueue = new ArrayBlockingQueue<QueryAndResult>(10000);
//    String caseDir = "/home/gzx/aliMiddleWare/prerun_data/case.0";
    String caseDir = "/home/gjl_workspace/lh_workspace/data/prerun_data/case.0";

    {
        orderFiles = new ArrayList<String>();
        goodFiles = new ArrayList<String>();
        buyerFiles = new ArrayList<String>();
        storeFolders = new ArrayList<String>();
        String base = "/home/gjl_workspace/lh_workspace/data/prerun_data/";
        if (debugTesting == true) {
            goodFiles.add(base+"good.0.0");
            goodFiles.add(base+"good.1.1");
            goodFiles.add(base+"good.2.2");

            buyerFiles.add(base+"buyer.0.0");
            buyerFiles.add(base+"buyer.1.1");

            orderFiles.add(base+"order.0.0");
            orderFiles.add(base+"order.0.3");
            orderFiles.add(base+"order.1.1");
            orderFiles.add(base+"order.2.2");
            storeFolders.add(base+"result");
        }
    }

    public static void main(String[] args) {

        TestOrderSystem testOrderSystem = new TestOrderSystem();
        testOrderSystem.testOrderSystem();
    }

    public void testOrderSystem() {
        ExecutorService execotr = Executors.newFixedThreadPool(20);
        CompletionService<Boolean> completionService = new ExecutorCompletionService<Boolean>(execotr);
        final OrderSystemImpl orderSystem = new OrderSystemImpl();


        long startConstruct = System.currentTimeMillis();
        long endOfConstruct;
        try {
            System.out.println("!!!! start contructing " + startConstruct);
            orderSystem.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
            endOfConstruct = System.currentTimeMillis();
            System.out.println("!!!! end of constructing " + endOfConstruct + " consumes ms " + (endOfConstruct - startConstruct));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


//        orderSystem.queryOrdersByBuyer(1459406100,1471091003,"wx-ac3f-a3b1676cd253");
//        BUYERID:wx-8dcb-78e54418427a
//        STARTTIME:1464501838
//        ENDTIME:1473813691
        orderSystem.queryOrdersByBuyer(1464667545,1474579199,"tp-8284-614822321c10");
//        orderSystem.queryOrdersByBuyer(1469835149, 1485773444, "ap-9523-f8d17a93670c");
        orderSystem.queryOrder(605145993, null);
        ArrayList<String> keysOrder = new ArrayList<String>();
//        keysOrder.add("*");
        final AtomicInteger querySums = new AtomicInteger(0);
        orderSystem.queryOrder(591087453, null);
        orderSystem.queryOrder(591087453, keysOrder);
        for (int i = 0; i < 20; i++) {

            final int finalI = i;
            completionService.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {

                    int sum = 0;
                    Long startTime = System.currentTimeMillis();
                    while (!Thread.currentThread().isInterrupted()) {
//                        if (querySums.get() >= 390000) {
//                            return true;
//                            System.exit(2);
//                        }
                        QueryAndResult query = QueryQueue.take();
                        try {
                            if (query != null) {
                                if (query.type == QueryAndResult.Query_Type.QUERY_GOOD_SUM) {

                                    KeyValue keyvalue = orderSystem.sumOrdersByGood(query.goodid, query.sumkey);
                                    querySums.incrementAndGet();
                                    try {

//                                        if (!keyvalue.key().equals(query.sumkey) || !(Math.abs(query.sumValue - keyvalue.valueAsDouble()) < 0.0001)) {
//                                            System.out.println(" Query good sum error " + query.goodid + " sum key is " + query.sumkey);
//                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                } else if (query.type == QueryAndResult.Query_Type.QUERY_SALER_GOOD) {
                                    Iterator<Result> iter = orderSystem.queryOrdersBySaler(query.sailerId, query.goodid, null);
                                    querySums.incrementAndGet();
//                                    if (checkResult(iter, query.results) == false) {
//                                        System.out.println(" QUERY SALER GOOD ERROR " + query.sailerId + " " + query.goodid);
//                                    }
                                } else if (query.type == QueryAndResult.Query_Type.QUERY_BUYER_TRANGE) {
                                    Iterator<Result> iter = orderSystem.queryOrdersByBuyer(query.startTime, query.endTime, query.buyerId);
                                    querySums.incrementAndGet();
                                    compareNum(iter,query.results);
//                                    if (checkResult(iter, query.results) == false) {
//                                        System.out.println(" QUERY BUYER TSRANGE " + query.startTime + " " + query.endTime + " " + query.buyerId);
//                                    }
                                } else if (query.type == QueryAndResult.Query_Type.QUERY_ORDER) {
                                    Result result = orderSystem.queryOrder(query.orderId, null);
                                    querySums.incrementAndGet();
//                                    if (result.orderId() != query.orderId) {
//                                        System.out.println("QUERY ORDER error");
//                                    }
                                    KeyValue[] keyValues = result.getAll();
//                                    for (Map.Entry<String, String> entrySet : query.result.entrySet()) {
//                                        Boolean findedInResult = false;
//                                        for (KeyValue keyValue : keyValues) {
//                                            if (entrySet.getKey().equals(keyValue.key())) {
//                                                findedInResult = true;
//                                                if (!entrySet.getValue().equals(keyValue.valueAsString())) {
//                                                    System.out.println("QUERY_ORDER ERROR " + query.orderId);
//                                                }
//                                                break;
//                                            }
//                                        }
//                                        if (findedInResult == false) {
//                                            System.out.println("QUERY_ORDER ERROR " + query.orderId);
//                                        }
//                                    }
                                }
                            }
                            sum++;
                            if (sum % 1000 == 0) {
                                System.out.println("!!! consumed " + (System.currentTimeMillis() - startTime));
                                System.out.println("thread " + finalI + " total querys " + sum);
                            }
                        } catch (Exception e) {
//                        Thread.sleep(10000);
//                            System.out.println(query.toString());
                            e.printStackTrace();
//                        break;
                        }
                    }
                    return true;
                }
            });

        }
        Scanner scanner = null;
        try {
            scanner = new Scanner(new BufferedInputStream(new FileInputStream(caseDir)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            QueryAndResult queryAndResult;
            if (line.startsWith("CASE:")) {
                if (line.contains("QUERY_ORDER")) {
                    queryAndResult = new QueryAndResult(QueryAndResult.Query_Type.QUERY_ORDER);
                    String orderIdLine = scanner.nextLine();
                    long orderId = Integer.parseInt(orderIdLine.substring(orderIdLine.indexOf(":") + 1));
                    queryAndResult.setOrderId(orderId);
                    String keyLine = scanner.nextLine();
                    String keys = keyLine.substring(keyLine.indexOf("[") + 1, keyLine.lastIndexOf("]"));
                    String resultLines = scanner.nextLine();
                    resultLines = scanner.nextLine();
                    if (resultLines.equals("}")) {
                        queryAndResult.result = new HashMap<String, String>();
                    } else {

                        resultLines = resultLines.substring(resultLines.indexOf("[") + 1, resultLines.length() - 2);
                        String[] splits = resultLines.split(",");
                        HashMap<String, String> res = new HashMap<String, String>();
                        if (!resultLines.equals("")) {
                            for (String split :
                                    splits) {
                                String[] keyvalue = split.split(":");
                                res.put(keyvalue[0], keyvalue[1]);
                            }
                        }
                        queryAndResult.result = res;
                        scanner.nextLine();
                    }
                    try {
                        QueryQueue.put(queryAndResult);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else if (line.contains("QUERY_GOOD_SUM")) {
                    queryAndResult = new QueryAndResult(QueryAndResult.Query_Type.QUERY_GOOD_SUM);
                    String goodIdLine = scanner.nextLine();
                    goodIdLine = goodIdLine.substring(goodIdLine.indexOf(":") + 1);
                    queryAndResult.setGoodid(goodIdLine);
                    String keysLine = scanner.nextLine();
                    keysLine = keysLine.substring(keysLine.indexOf("[") + 1, keysLine.length() - 1);
                    queryAndResult.setSumkey(keysLine);
                    String resultLine = scanner.nextLine();
                    resultLine = resultLine.substring(resultLine.indexOf(":") + 1);
                    if (resultLine.equals("null")) {
                        queryAndResult.setSumValue(Double.valueOf(-1));
                    } else {
                        queryAndResult.setSumValue(Double.parseDouble(resultLine));
                    }
                    try {
                        QueryQueue.put(queryAndResult);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else if (line.contains("QUERY_BUYER_TSRANGE")) {
                    queryAndResult = new QueryAndResult(QueryAndResult.Query_Type.QUERY_BUYER_TRANGE);
                    String buyerIdLine = scanner.nextLine();
                    buyerIdLine = buyerIdLine.substring(buyerIdLine.indexOf(":") + 1);
                    queryAndResult.setBuyerId(buyerIdLine);
                    String startTime = scanner.nextLine();
                    startTime = startTime.substring(startTime.indexOf(":") + 1);
                    queryAndResult.setStartTime(Long.parseLong(startTime));
                    String endTimeLine = scanner.nextLine();
                    endTimeLine = endTimeLine.substring(endTimeLine.indexOf(":") + 1);
                    queryAndResult.setEndTime(Long.parseLong(endTimeLine));
                    String resultLines = scanner.nextLine();
                    resultLines = scanner.nextLine();
                    while (!resultLines.equals("}")) {
                        String orderId = resultLines.substring(0, resultLines.indexOf(","));
                        String kv = resultLines.substring(resultLines.indexOf(",") + 1);
                        orderId = orderId.substring(orderId.indexOf(":") + 1);
                        kv = kv.substring(kv.indexOf("[") + 1, kv.indexOf("]"));
                        HashMap<String, String> result = new HashMap<String, String>();
                        for (String split : kv.split(",")) {
                            String[] kpair = split.split(":");
                            result.put(kpair[0], kpair[1]);
                        }
                        queryAndResult.getResults().put(orderId, result);
                        resultLines = scanner.nextLine();
                    }
                    try {
                        QueryQueue.put(queryAndResult);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else if (line.contains("QUERY_SALER_GOOD")) {
                    queryAndResult = new QueryAndResult(QueryAndResult.Query_Type.QUERY_SALER_GOOD);
                    String salerIdLine = scanner.nextLine();
                    salerIdLine = salerIdLine.substring(salerIdLine.indexOf(":") + 1);
                    queryAndResult.setSailerId(salerIdLine);
                    String goodidLine = scanner.nextLine();
                    goodidLine = goodidLine.substring(goodidLine.indexOf(":") + 1);
                    queryAndResult.setGoodid(goodidLine);
                    String keyline = scanner.nextLine();
                    String resultLines = scanner.nextLine();
                    resultLines = scanner.nextLine();
                    while (!resultLines.equals("}")) {
                        String orderId = resultLines.substring(0, resultLines.indexOf(","));
                        String kv = resultLines.substring(resultLines.indexOf(",") + 1);
                        orderId = orderId.substring(orderId.indexOf(":") + 1);
                        kv = kv.substring(kv.indexOf("[") + 1, kv.indexOf("]"));
                        HashMap<String, String> result = new HashMap<String, String>();
                        if (!kv.equals("")) {
                            for (String split : kv.split(",")) {
                                String[] kpair = split.split(":");
                                result.put(kpair[0], kpair[1]);
                            }
                        }
                        queryAndResult.getResults().put(orderId, result);
                        resultLines = scanner.nextLine();
                    }
                    try {
                        QueryQueue.put(queryAndResult);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        scanner.close();

        for (int i = 0; i < 20; i++) {
            try {
                completionService.take().get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        execotr.shutdownNow();

    }


    private boolean checkResult(Iterator<Result> iter, HashMap<String, HashMap<String, String>> correct) {
        while (iter.hasNext()) {
            Result result = iter.next();
            HashMap<String, String> values = correct.get(String.valueOf(result.orderId()));
            if (values == null) {
                return false;
            }
            for (KeyValue keyvalue : result.getAll()) {
                if (values.get(keyvalue.key()) == null) {
                    return false;
                }
                if (!values.get(keyvalue.key()).equals(keyvalue.valueAsString())) {
                    if(!keyvalue.valueAsString().contains(values.get(keyvalue.key()))){
                        return false;
                    }
                }
            }
        }
        return true;
    }
     private boolean compareNum(Iterator<Result> iter, HashMap<String, HashMap<String, String>> correct) {
         int sum=0;
        while (iter.hasNext()) {
            iter.next();
            sum++;
        }
         if(sum!=correct.size()){
             System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
         }
        return true;
    }
}