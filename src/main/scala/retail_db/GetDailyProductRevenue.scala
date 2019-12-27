package retail_db

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by itversity on 10/08/18.
 */
object GetDailyProductRevenue {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    val spark = SparkSession.
      builder.
      appName("Daily Product Revenue").
      master(envProps.getString("execution.mode")).
      getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    import spark.implicits._

    val inputBaseDir = envProps.getString("input.base.dir")
    val orders = spark.
      read.
      schema("""
         order_id INT,
         order_date STRING,
         order_customer_id INT,
         order_status STRING
         """).
      csv(inputBaseDir + "/orders")
    val orderItems = spark.
      read.
      option("inferSchema", "true").
      schema("""
         order_item_id INT,
         order_item_order_id INT,
         order_item_product_id INT,
         order_item_quantity INT,
         order_item_subtotal FLOAT,
         order_item_product_price FLOAT
         """).
      csv(inputBaseDir + "/order_items")

    val dailyProductRevenue = orders.where("order_status in ('CLOSED', 'COMPLETE')").
      join(orderItems, $"order_id" === $"order_item_order_id").
      groupBy("order_date", "order_item_product_id").
      agg(round(sum($"order_item_subtotal"), 2).alias("revenue")).
      orderBy($"order_date", $"revenue" desc)

    val outputBaseDir = envProps.getString("output.base.dir")
    dailyProductRevenue.
      write.
      mode("overwrite").
      json(outputBaseDir + "/daily_product_revenue")
  }

}