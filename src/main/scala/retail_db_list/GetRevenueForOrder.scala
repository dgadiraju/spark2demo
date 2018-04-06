package retail_db_list

/**
  * Created by itversity on 05/04/18.
  */
object GetRevenueForOrder {
  def main(args: Array[String]): Unit = {
    val orderItemsPath = args(0)
    val orderId = args(1).toInt

    val orderItems = scala.io.Source.fromFile(orderItemsPath).
      getLines().toList
    //    orderItems.take(10).foreach(println)
    val orderItemsFiltered = orderItems.
      filter(e => e.split(",")(1).toInt == orderId)
    //    orderItemsFiltered.foreach(println)

    val orderItemsSubtotals = orderItemsFiltered.
      map(e => e.split(",")(4).toFloat)
    //    orderItemsSubtotals.foreach(println)

    val orderRevenue = orderItemsSubtotals.reduce((curr, next) => curr + next)
    println("Order revenue for order id " + orderId + " is " + orderRevenue)
  }

}
