package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * 交易订单系统接口
 * 
 * @author wangxiang@alibaba-inc.com
 */
public interface OrderSystem {

  /**
   * 测试程序调用此接口构建交易订单记录查询系统
   * 
   * 输入数据格式请看README.md
   * 
   * @param orderFiles
   *          订单文件列表
   * @param buyerFiles
   *          买家文件列表
   * @param goodFiles
   *          商品文件列表
   * 
   * @param storeFolders
   *          存储文件夹的目录，保证每个目录是有效的，每个目录在不同的磁盘上
   */
  public void construct(Collection<String> orderFiles,
                        Collection<String> buyerFiles, Collection<String> goodFiles,
                        Collection<String> storeFolders) throws IOException, InterruptedException;

  class TypeException extends Exception {
    private static final long serialVersionUID = -5723782756972021205L;
  }

  /**
   * 用于访问记录中的Key-Value接口
   */
  interface KeyValue {
    /**
     * 字段名
     * @return
     */
    public String key();

    /**
     * 返回字符串形式的值
     * @return
     * @throws TypeException
     */
    public String valueAsString();

    /**
     * 返回long形式的值
     * 类型转换失败则抛出TypeException异常
     * @return
     * @throws TypeException
     */
    public long valueAsLong() throws TypeException;

    /**
     * 返回double形式的值
     * 测试程序邀请浮点数精度保证至小数点后6位
     * 类型转换失败则抛出TypeException异常
     */
    public double valueAsDouble() throws TypeException;

    /**
     * 只有 true, false字符串能被转换为boolean类型，其他值调用asBoolean则抛出TypeException [String]
     * true -> [Boolean] true [String]
     * false -> [Boolean] false [String]
     * True -> TypeException [String] 
     * False -> TypeException
     */
    public boolean valueAsBoolean() throws TypeException;
  }

  /**
   * 用于访问单条订单记录的接口
   */
  interface Result {
    /**
     * 如果返回数据中不存该key，则返回null
     * 
     * @param key
     *          需要获取的订单字段
     * @return
     */
    public KeyValue get(String key);

    /**
     * 获取所有返回结果中的KV对
     * 
     * @return
     */
    KeyValue[] getAll();

    public long orderId();
  }

  /**
   * 查询订单号为orderid的指定字段
   * 
   * @param orderid
   *          订单号
   * @param keys
   *          待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
   * @return 查询结果，如果该订单不存在，返回null
   */
  Result queryOrder(long orderId, Collection<String> keys);

  /**
   * 查询某位买家createtime字段从[startTime, endTime) 时间范围内发生的所有订单的所有信息
   * 
   * @param startTime 订单创建时间的下界
   * @param endTime 订单创建时间的上界
   * @param buyerid
   *          买家Id
   * @return 符合条件的订单集合，按照createtime大到小排列
   */
  Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
                                      String buyerid);

  /**
   * 查询某位卖家某件商品所有订单的某些字段
   * 
   * @param salerid 卖家Id
   * @param goodid 商品Id
   * @param keys 待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
   * @return 符合条件的订单集合，按照订单id从小至大排序
   */
  Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
                                      Collection<String> keys);

  /**
   * 对某件商品的某个字段求和，只允许对long和double类型的KV求和 如果字段中既有long又有double，则使用double
   * 如果求和的key中包含非long/double类型字段，则返回null 如果查询订单中的所有商品均不包含该字段，则返回null
   * 
   * @param goodid 商品Id
   * @param key 求和字段
   * @return 求和结果
   */
  KeyValue sumOrdersByGood(String goodid, String key);
}
