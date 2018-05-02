import pl.mpiniarski.distributedmonitor.DistributedMonitor
import pl.mpiniarski.distributedmonitor.communication.Messenger
import pl.mpiniarski.distributedmonitor.communication.ZeroMqBinaryMessenger
import java.util.*

class Buffer(private val size : Int, messenger : Messenger)
    : DistributedMonitor("buffer", messenger) {

    private val empty = createCondition("empty")
    private val full = createCondition("full")
    private var values : Stack<Int> = Stack()

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

    override fun serializeState() : ByteArray {
        return values.joinToString(",").toByteArray()
    }

    override fun deserializeAndUpdateState(state : ByteArray) {
        val stack = Stack<Int>()
        val string = String(state)
        if (!string.isEmpty()) {
            stack.addAll(string.split(',').map { it.toInt() }.toList())
        }
        values = stack
    }

}

fun main(args : Array<String>) {

    System.out.println("START producer")

    val zeroMqBinaryMessenger = ZeroMqBinaryMessenger("localhost:5551", listOf("localhost:5550"))
    val messenger = Messenger(zeroMqBinaryMessenger)
    val buffer = Buffer(1, messenger)

    messenger.start()

    for (i in 1 .. 100) {
        buffer.produce(i)
        System.out.println("Produced $i")
    }

    System.out.println("STOP producer")

    messenger.close()
}

