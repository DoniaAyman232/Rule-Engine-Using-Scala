
import java.io.File
import java.time.LocalDate
import scala.io.Source
import java.io.{FileOutputStream, PrintWriter}
import java.sql.{ Date, DriverManager}
import java.util.logging.{Logger, FileHandler, SimpleFormatter}





case class Order(timestamp: String, product_name: String, expiry_date: String, quantity: String, unit_price: String, channel: String, payment_method: String)

object Main extends App {


  val logger = Logger.getLogger("MainLogger")
  val fileHandler = new FileHandler("main.log")
  fileHandler.setFormatter(new SimpleFormatter())
  logger.addHandler(fileHandler)

  logger.info("Starting the program...")

  val orders: List[Order] = {
    val file = new File("src/main/resources/TRX1000.csv")
    val lines = Source.fromFile(file).getLines().toList.tail
    lines.map { line =>
      val Array(timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method) = line.split(",")
      Order(timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method)
    }
  }


  def calculateExpiryDateDiscount(order: Order): Double = {
    val transactionDate = LocalDate.parse(order.timestamp.substring(0, 10))
    val expirationDate = LocalDate.parse(order.expiry_date)
    val remainingDays = java.time.temporal.ChronoUnit.DAYS.between(transactionDate, expirationDate).toInt

    remainingDays match {
      case x if x >= 1 && x <= 29 => 0.30 - x * 0.01
      case _ => 0.0
    }
  }


  def cheeseAndWineDiscount(order: Order): Double = {
    if (order.product_name.startsWith("Wine")) 0.05
    else if (order.product_name.startsWith("Cheese")) 0.10
    else 0.0
  }

  def specialDate(timestamp: String): Boolean = {
    val date = LocalDate.parse(timestamp.substring(0, 10))
    date.getMonthValue == 3 && date.getDayOfMonth == 23

  }

  def marchSpecialDate(order: Order): Double = {
    if (specialDate(order.timestamp)) 0.50
    else 0.0
  }


  def boughtMoreQuantity(order: Order): Double = {
    val quantity = order.quantity.toInt
    quantity match {
      case q if q >= 6 && q <= 9 => 0.05
      case q if q >= 10 && q <= 14 => 0.07
      case q if q >= 15 => 0.10
      case _ => 0.0
    }
  }

  def visadiscount(order: Order): Double = {
    if (order.payment_method == "Visa") 0.05
    else 0.0
  }

  def appdiscount(order: Order): Double = {
    if (order.channel == "App") {
      val roundedQuantity = math.ceil(order.quantity.toDouble / 5) * 5
      roundedQuantity match {
        case q if q <= 5 => 0.05
        case q if q <= 10 => 0.10
        case q if q <= 15 => 0.15
        case _ => 0.20
      }
    } else {
      0
    }
  }


  def applydiscount(order: Order): Double = {
    val discounts = List(
      calculateExpiryDateDiscount(order),
      cheeseAndWineDiscount(order),
      marchSpecialDate(order),
      boughtMoreQuantity(order),
      visadiscount(order),
      appdiscount(order)
    )



    val ZeroDiscounts = discounts.filter(_ != 0.0)
    val topTwoDiscounts = ZeroDiscounts.sorted.reverse.take(2)

    if (topTwoDiscounts.length >= 2) {
      val avgDiscount = topTwoDiscounts.sum / 2.0
      avgDiscount
    } else if (topTwoDiscounts.length == 1) {
      topTwoDiscounts.head
    } else {
      0.0
    }


  }

 // val numOrdersWithDiscount = orders.count(order => applydiscount(order) > 0.0)

 // println(s" $numOrdersWithDiscount")

  def finaloutput(order: Order): String = {
    val discount = applydiscount(order)
    val quantity = order.quantity.toInt
    val unitPrice = order.unit_price.toDouble
    val discountedPrice = quantity * unitPrice * (1 - discount)

    s"${order.timestamp}, ${order.product_name}, ${order.expiry_date}, ${order.quantity}, ${order.unit_price}, ${order.channel}, ${order.payment_method}, ${f"${discount * 100}%.1f"}, $discountedPrice"
  }

  val f: File = new File("src/main/resources/retail_store.csv")
  val writer = new PrintWriter(new FileOutputStream(f, true))

  def writeLine(line: String): Unit = {
    writer.println(line)
  }


  orders.foreach { order =>
    val output = finaloutput(order)
    writeLine(output)
  }

  writer.close()

  val url = "jdbc:oracle:thin:@//localhost:1521/XE"
  val username = "scala"
  val password = "scala"


  val connection = DriverManager.getConnection(url, username, password)


  val insertStatement = connection.prepareStatement(
    "INSERT INTO scala.orders (timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method, discount, finalprice) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
  )


  orders.foreach { order =>
    val output = finaloutput(order)
    val parts = output.split(", ")

    val transactionDate = Date.valueOf(LocalDate.parse(parts(0).substring(0, 10)))
    val expirationDate = Date.valueOf(LocalDate.parse(parts(2).substring(0, 10)))

    insertStatement.setDate(1, transactionDate)
    insertStatement.setString(2, parts(1))
    insertStatement.setDate(3, expirationDate)
    insertStatement.setInt(4, parts(3).toInt)
    insertStatement.setDouble(5, parts(4).toDouble)
    insertStatement.setString(6, parts(5))
    insertStatement.setString(7, parts(6))
    insertStatement.setDouble(8, parts(7).toDouble)
    insertStatement.setDouble(9, parts(8).toDouble)
    insertStatement.executeUpdate()
  }


  insertStatement.close()
  connection.close()


  def logDatabase(url: String, username: String, orders: List[Order]): Unit = {
    logger.info(s"Connecting to database with URL: $url and username: $username")

    orders.foreach { order =>
      logger.info(s"Inserting order into database: $order")
    }

    logger.info("Database insertion completed")
  }


  logDatabase(url, username, orders)
  logger.info("Ending the program...")

}