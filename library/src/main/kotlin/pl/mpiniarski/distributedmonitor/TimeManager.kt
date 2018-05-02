package pl.mpiniarski.distributedmonitor

import java.lang.Integer.max

class TimeManager(nodes : List<String>) {
    var localTime = 0
    private val timestamps : MutableMap<String, Int> = nodes.map { it to 0 }.toMap().toMutableMap()

    fun notifyEvent() {
        localTime += 1
    }

    fun notifySync(syncValue : Int, syncHost : String) {
        timestamps[syncHost] = syncValue
        localTime = max(localTime, syncValue) + 1
    }

    fun allRemoteLaterThen(timestamp : Int) : Boolean {
        return timestamps
                .map { it.value }
                .all { it > timestamp }
    }
}