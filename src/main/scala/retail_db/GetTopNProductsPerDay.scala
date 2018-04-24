package retail_db

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by itversity on 10/04/18.
  * Problem Statement: Get top n products per day by revenue
  * Consider only COMPLETE and CLOSED orders
  * Output: order_date, product_name, revenue
  * Creating Spark configuration and Spark Context
  * Reading data (from file system or database)
  * Row level transformations (filter, data cleansing and standardization)
  * Shuffle operations (joins, aggregations, sorting etc)
  * Row level transformations
  * Writing processed data (to file system or database) - Actions
  */

object GetTopNProductsPerDay {
  def getTopNProductsForDay(productsForDay: (String, List[(Int, Float)]), topN: Int) = {
    productsForDay._2.sortBy(k => -k._2).take(5).map(e => (productsForDay._1, e))
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster(args(0)).
      setAppName("Get top n products per day by revenue")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // Reading data (from file system or database)
    // Read orders, order_items and products

    val inputBaseDir = args(1)
    val orders = sc.textFile(inputBaseDir + "/orders")
    val orderItems = sc.textFile(inputBaseDir + "/order_items")
    val products = sc.textFile(inputBaseDir + "/products")

    // Row level transformation
    // orders - filter and apply map to transform data
    // "1,2013-07-25 00:00:00.0,11599,CLOSED" -> (1, "2013-07-25 00:00:00.0")
    val ordersMap = orders.
      filter(e => List("COMPLETE", "CLOSED").contains(e.split(",")(3))).
      map(e => (e.split(",")(0).toInt, e.split(",")(1)))

    // order_items - use map to transform the data
    // "1,1,957,1,299.98,299.98" -> (1, (957, 299.98))

    val orderItemsMap = orderItems.
      map(e => (e.split(",")(1).toInt, (e.split(",")(2).toInt, e.split(",")(4).toFloat)))

    val regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    val productsMap = products.
      map(e => (e.split(regex, -1)(0).toInt, e.split(regex, -1)(2)))

    // orders join order_items -> 364 * 1000, (1, ("2013-07-25 00:00:00.0", (957, 299.98)))
    val ordersJoin = ordersMap.join(orderItemsMap).
      map(e => ((e._2._1, e._2._2._1), e._2._2._2))
    // (("2013-07-25 00:00:00.0", 957), 299.98)

    // compute revenue per day per product -> 364 * 100
    val revenuePerDayPerProductId = ordersJoin.
      reduceByKey((curr, next) => curr + next)
    val productIdAndRevenuePerDay = revenuePerDayPerProductId.
      map(e => (e._1._1, (e._1._2, e._2)))

    // get top n products per day -> 364 * 5
    val productsPerDay = productIdAndRevenuePerDay.groupByKey
    val topNProductIdsPerDay = productsPerDay.
      flatMap(e => getTopNProductsForDay((e._1, e._2.toList), args(3).toInt)).
      map(e => (e._2._1, (e._1, e._2._2)))

    // join with products ->
    // date, product_name and revenue
    val topNProductsPerDay = productsMap.join(topNProductIdsPerDay).
      map(e => {
        ((e._2._2._1, -e._2._2._2), e._2._2._1 + "\t" + e._2._1 + "\t" + e._2._2._2)
      }).
      sortByKey().
      map(e => e._2)
    topNProductsPerDay.saveAsTextFile(args(2))

    // Not as efficient as above
    // orders join order_items -> 364 * 1000
    // join with products and get date, product_name, order_item_subtotal -> 364 * 1000
    // compute revenue per day per product -> 364 * 100
    // get top n products per day -> 364 * 5
    // date, product_name and revenue



  }

}
