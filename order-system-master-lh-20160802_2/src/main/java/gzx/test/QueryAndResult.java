package gzx.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

/**
 * Created by gzx on 16-7-19.
 */
public class QueryAndResult {
   public static enum Query_Type{
        QUERY_ORDER,
        QUERY_SALER_GOOD,
        QUERY_BUYER_TRANGE,
        QUERY_GOOD_SUM,
    }

    Query_Type type;
    Long orderId;
    String buyerId;
    Long startTime;
    Long endTime;
    String sailerId;
    String goodid;
    String sumkey;
    Double sumValue;
    Collection<String> keys=null;

    public Double getSumValue() {
        return sumValue;
    }

    public void setSumValue(Double sumValue) {
        this.sumValue = sumValue;
    }

    HashMap<String,String> result;


    public HashMap<String, HashMap<String, String>> getResults() {
        return results;
    }

    public void setResults(HashMap<String, HashMap<String, String>> results) {
        this.results = results;
    }

    HashMap<String,HashMap<String,String>> results=new HashMap<String, HashMap<String, String>>();

    public QueryAndResult(Query_Type type){
        this.type=type;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getBuyerId() {
        return buyerId;
    }

    public void setBuyerId(String buyerId) {
        this.buyerId = buyerId;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public String getSailerId() {
        return sailerId;
    }

    public void setSailerId(String sailerId) {
        this.sailerId = sailerId;
    }

    public String getGoodid() {
        return goodid;
    }

    public void setGoodid(String goodid) {
        this.goodid = goodid;
    }

    public String getSumkey() {
        return sumkey;
    }

    public void setSumkey(String sumkey) {
        this.sumkey = sumkey;
    }

    public void addKeys(String key) {
        if(keys==null){
            keys=new ArrayList<String>();
        }
        keys.add(key);
    }

    public Collection<String> getKeys() {
        return keys;
    }

    @Override
    public String toString() {
        return "QueryAndResult{" +
                "type=" + type +
                ", orderId=" + orderId +
                ", buyerId='" + buyerId + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", sailerId='" + sailerId + '\'' +
                ", goodid='" + goodid + '\'' +
                ", sumkey='" + sumkey + '\'' +
                ", sumValue=" + sumValue +
                ", keys=" + keys +
                ", result=" + result +
                ", results=" + results +
                '}';
    }
}