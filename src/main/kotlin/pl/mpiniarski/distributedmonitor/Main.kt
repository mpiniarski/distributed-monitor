package pl.mpiniarski.distributedmonitor

import mu.KotlinLogging
import pl.mpiniarski.distributedmonitor.communication.Messenger
import pl.mpiniarski.distributedmonitor.communication.ZeroMqBinaryMessenger
import java.util.*


class Buffer(private val size : Int, messenger : Messenger)
    : DistributedMonitor("buffer", messenger) {

    private val logger = KotlinLogging.logger { }

    private val empty = createCondition("empty")
    private val full = createCondition("full")
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


    val zeroMqBinaryMessenger = ZeroMqBinaryMessenger("", listOf(""))
    val messenger = Messenger(zeroMqBinaryMessenger)

    val buffer = Buffer(1, messenger)

    val producer = Producer(buffer)
    val consumer = Consumer(buffer)

    producer.start()
    consumer.start()
}
