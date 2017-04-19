package cn.edu.nj.streaming
import java.io.{PrintWriter}
import java.net.ServerSocket
import scala.io.Source
/**
  * Created by apple on 17/4/19.
  */
object SaleSimulation {
  def index(length: Int) = {
    import java.util.Random
    val rdm = new Random

    rdm.nextInt(length)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("Usage:<filename> <port> <millisecond>")
      System.exit(1)
    }

    val filename = args(0)
    val lines = Source.fromFile(filename).getLines.toList
    val filerow = lines.length

    val listener = new ServerSocker(args(1).toInt)
    while (true) {
      //once the connect appear
      val socket = listener.accept()
      new Thread() {
        override def run = {
          println("got cliented from:" + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)
          while (true) {
            Thread.sleep(args(2).toLong)
            val content = lines(index(filerow))
            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
