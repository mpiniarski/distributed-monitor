package pl.mpiniarski.distributedmonitor

import mu.KotlinLogging
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Stack


fun main(args: Array<String>) {
    println("kotlin is great!")

    val buffer = Buffer(1)

    val producer = Producer(buffer)
    val consumer = Consumer(buffer)

    producer.start()
    consumer.start()
}

class Producer(val buffer: Buffer): Thread() {
  private val logger = KotlinLogging.logger { }

    public override fun run() {
        for(i in 1..10){
            buffer.produce(i)
            logger.info("Prodced $i")
        }
    }
}

class Consumer(val buffer: Buffer) : Thread() {
  private val logger = KotlinLogging.logger { }

    public override fun run() {
        for(i in 1..10){
            val value = buffer.consume()
            logger.info("Consumed $value")
        }
    }
}

class Buffer(val size: Int) : DistributedMonitor() {
  private val logger = KotlinLogging.logger { }

  private val empty = createCondition()
  private val full = createCondition()
  val values : Stack<Int> = Stack()

  fun produce(value: Int) = entry {
      if (values.size == size) {
          full.await()
      }
      values.push(value)
      if (values.size == 1) {
          empty.signal()
       }
    }

  fun consume() : Int = entry {
      if (values.size == 0){
          empty.await()
      }
      val value = values.pop()
      if (values.size == size-1){
          full.signal()
      }
      return value
  }
}

open class DistributedMonitor  {
  protected val lock = ReentrantLock()

  protected fun createCondition() : Condition{
      return lock.newCondition()
  }

  protected inline fun <T> entry(f: () -> T) : T {
      try {
        lock.lock()
        return f()
      }
      finally {
          lock.unlock()
      }
  }
}
