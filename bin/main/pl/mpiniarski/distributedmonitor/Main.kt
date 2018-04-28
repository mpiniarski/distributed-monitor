package pl.mpiniarski.distributedmonitor

import mu.KotlinLogging
import java.util.*


class Buffer(private val size : Int, nodes : List<String>)
    : DistributedMonitor(ZeroMqCommunicator("", listOf(""))) {

    private val logger = KotlinLogging.logger { }

    private val empty = createCondition()
    private val full = createCondition()
    private val values : Stack<Int> = Stack()

    fun produce(value : Int) = entry {
        if (values.size == size) {
            full.await()
        }
        values.push(value)
        if (values.size == 1) {
            empty.signal()
        }
    }

    fun consume() : Int = entry {
        if (values.size == 0) {
            empty.await()
        }
        val value = values.pop()
        if (values.size == size - 1) {
            full.signal()
        }
        return value
    }
}

class Producer(private val buffer : Buffer) : Thread() {
    private val logger = KotlinLogging.logger { }

    override fun run() {
        for (i in 1 .. 10) {
            buffer.produce(i)
            logger.info("Produced $i")
        }
    }

}

class Consumer(private val buffer : Buffer) : Thread() {
    private val logger = KotlinLogging.logger { }

    override fun run() {
        for (i in 1 .. 10) {
            val value = buffer.consume()
            logger.info("Consumed $value")
        }
    }

}

fun main(args : Array<String>) {
    println("kotlin is great!")

    val buffer = Buffer(1, listOf())

    val producer = Producer(buffer)
    val consumer = Consumer(buffer)

    producer.start()
    consumer.start()
}
