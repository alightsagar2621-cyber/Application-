package com.example.data.mesh

import android.Manifest
import android.content.Context
import android.os.Build
import com.google.android.gms.nearby.Nearby
import com.google.android.gms.nearby.connection.AdvertisingOptions
import com.google.android.gms.nearby.connection.ConnectionInfo
import com.google.android.gms.nearby.connection.ConnectionLifecycleCallback
import com.google.android.gms.nearby.connection.ConnectionResolution
import com.google.android.gms.nearby.connection.ConnectionsClient
import com.google.android.gms.nearby.connection.DiscoveredEndpointInfo
import com.google.android.gms.nearby.connection.DiscoveryOptions
import com.google.android.gms.nearby.connection.EndpointDiscoveryCallback
import com.google.android.gms.nearby.connection.Payload
import com.google.android.gms.nearby.connection.PayloadCallback
import com.google.android.gms.nearby.connection.PayloadTransferUpdate
import com.google.android.gms.nearby.connection.Strategy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.json.JSONArray
import org.json.JSONObject
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * REAL phone-to-phone mesh using Google Nearby Connections (BLE + Wi-Fi Direct, no internet).
 *
 * Features:
 *  - Every phone advertises AND discovers (P2P_CLUSTER = many-to-many mesh)
 *  - Auto-connect (tie-break so both sides don't request at once)
 *  - Flooding relay with TTL + duplicate filter  -> multi-hop (A -> B -> C)
 *  - Heartbeat every 3s; no signal for 10s = peer treated as LOST -> [disconnectEvents]
 *
 * Gradle:  implementation("com.google.android.gms:play-services-nearby:19.3.0")
 *
 * Usage:
 *   val mesh = NearbyMeshManager(context, scope, "Sagar")
 *   // after runtime permissions granted:
 *   mesh.start()
 *   mesh.broadcast("SOS", "Bachcha kho gaya, Sector 3")
 *   scope.launch { mesh.disconnectEvents.collect { name -> /* siren + vibrate + UI alert */ } }
 *   scope.launch { mesh.incoming.collect { msg -> /* show in feed */ } }
 */
data class MeshMsg(
    val id: String,
    val type: String,       // "SOS", "MSG", "HB" (heartbeat, internal)
    val from: String,
    val text: String,
    val ttl: Int,
    val path: List<String>
) {
    val hops: Int get() = path.size
}

class NearbyMeshManager(
    context: Context,
    private val scope: CoroutineScope,
    userName: String
) {
    // Unique per run so tie-break and logs are unambiguous
    val myName: String = "$userName-${UUID.randomUUID().toString().take(4).uppercase()}"

    private val client: ConnectionsClient = Nearby.getConnectionsClient(context.applicationContext)
    private val strategy = Strategy.P2P_CLUSTER
    private val serviceId = "com.aistudio.kumbhmesh.mesh"

    private val peerNames = ConcurrentHashMap<String, String>()   // endpointId -> name (connected)
    private val pendingNames = ConcurrentHashMap<String, String>() // endpointId -> name (connecting)
    private val connecting = ConcurrentHashMap.newKeySet<String>()
    private val lastHeard = ConcurrentHashMap<String, Long>()
    private val seen = ConcurrentHashMap.newKeySet<String>()

    private val _peers = MutableStateFlow<List<String>>(emptyList())
    val peers: StateFlow<List<String>> = _peers.asStateFlow()

    private val _status = MutableStateFlow("Idle")
    val status: StateFlow<String> = _status.asStateFlow()

    private val _incoming = MutableSharedFlow<MeshMsg>(extraBufferCapacity = 64)
    val incoming: SharedFlow<MeshMsg> = _incoming.asSharedFlow()

    /** Emits the peer's name every time a connected peer is lost / goes out of range. */
    private val _disconnectEvents = MutableSharedFlow<String>(extraBufferCapacity = 16)
    val disconnectEvents: SharedFlow<String> = _disconnectEvents.asSharedFlow()

    private var heartbeatJob: Job? = null

    // ---------------------------------------------------------------- lifecycle

    fun start() {
        _status.value = "Starting..."
        client.startAdvertising(
            myName, serviceId, lifecycleCallback,
            AdvertisingOptions.Builder().setStrategy(strategy).build()
        ).addOnSuccessListener { _status.value = "Advertising + searching" }
            .addOnFailureListener { _status.value = "Advertise failed: ${it.message}" }

        client.startDiscovery(
            serviceId, discoveryCallback,
            DiscoveryOptions.Builder().setStrategy(strategy).build()
        ).addOnFailureListener { _status.value = "Discovery failed: ${it.message}" }

        startHeartbeat()
    }

    fun stop() {
        heartbeatJob?.cancel()
        client.stopAdvertising()
        client.stopDiscovery()
        client.stopAllEndpoints()
        peerNames.clear(); pendingNames.clear(); connecting.clear(); lastHeard.clear()
        _peers.value = emptyList()
        _status.value = "Stopped"
    }

    // ---------------------------------------------------------------- sending

    /** Send to everyone in range; they relay further until TTL runs out. */
    fun broadcast(type: String, text: String, ttl: Int = 7): MeshMsg {
        val msg = MeshMsg(
            id = UUID.randomUUID().toString().take(8),
            type = type, from = myName, text = text, ttl = ttl, path = listOf(myName)
        )
        seen.add(msg.id)
        peerNames.keys.forEach { sendTo(it, msg) }
        return msg
    }

    private fun sendTo(endpointId: String, msg: MeshMsg) {
        val json = JSONObject()
            .put("id", msg.id).put("type", msg.type).put("from", msg.from)
            .put("text", msg.text).put("ttl", msg.ttl)
            .put("path", JSONArray(msg.path))
        client.sendPayload(endpointId, Payload.fromBytes(json.toString().toByteArray()))
    }

    private fun parse(bytes: ByteArray): MeshMsg? = try {
        val j = JSONObject(String(bytes))
        val p = j.getJSONArray("path")
        MeshMsg(
            id = j.getString("id"), type = j.getString("type"), from = j.getString("from"),
            text = j.getString("text"), ttl = j.getInt("ttl"),
            path = (0 until p.length()).map { p.getString(it) }
        )
    } catch (e: Exception) { null }

    // ---------------------------------------------------------------- heartbeat / loss detection

    private fun startHeartbeat() {
        heartbeatJob?.cancel()
        heartbeatJob = scope.launch(Dispatchers.Default) {
            while (isActive) {
                delay(3000)
                val hb = MeshMsg(UUID.randomUUID().toString().take(8), "HB", myName, "", 1, listOf(myName))
                peerNames.keys.forEach { sendTo(it, hb) }

                val now = System.currentTimeMillis()
                lastHeard.forEach { (id, t) ->
                    if (now - t > 10_000 && peerNames.containsKey(id)) {
                        client.disconnectFromEndpoint(id)
                        peerGone(id)
                    }
                }
            }
        }
    }

    /** Idempotent: fires the alert only once per peer. */
    private fun peerGone(endpointId: String) {
        val name = peerNames.remove(endpointId) ?: return
        lastHeard.remove(endpointId)
        _peers.value = peerNames.values.toList()
        _disconnectEvents.tryEmit(name)
    }

    // ---------------------------------------------------------------- Nearby callbacks

    private val discoveryCallback = object : EndpointDiscoveryCallback() {
        override fun onEndpointFound(endpointId: String, info: DiscoveredEndpointInfo) {
            if (peerNames.containsKey(endpointId)) return
            // Tie-break: only the "bigger" name sends the request, other side just accepts.
            if (myName <= info.endpointName) return
            if (!connecting.add(endpointId)) return
            client.requestConnection(myName, endpointId, lifecycleCallback)
                .addOnFailureListener { connecting.remove(endpointId) }
        }

        override fun onEndpointLost(endpointId: String) { /* connection callbacks handle the rest */ }
    }

    private val lifecycleCallback = object : ConnectionLifecycleCallback() {
        override fun onConnectionInitiated(endpointId: String, info: ConnectionInfo) {
            pendingNames[endpointId] = info.endpointName
            client.acceptConnection(endpointId, payloadCallback) // open mesh: auto-accept
        }

        override fun onConnectionResult(endpointId: String, result: ConnectionResolution) {
            connecting.remove(endpointId)
            val name = pendingNames.remove(endpointId) ?: endpointId
            if (result.status.isSuccess) {
                peerNames[endpointId] = name
                lastHeard[endpointId] = System.currentTimeMillis()
                _peers.value = peerNames.values.toList()
            }
        }

        override fun onDisconnected(endpointId: String) {
            peerGone(endpointId)
        }
    }

    private val payloadCallback = object : PayloadCallback() {
        override fun onPayloadReceived(endpointId: String, payload: Payload) {
            lastHeard[endpointId] = System.currentTimeMillis() // any packet = peer alive
            val bytes = payload.asBytes() ?: return
            val msg = parse(bytes) ?: return
            if (msg.type == "HB") return
            if (!seen.add(msg.id)) return               // duplicate filter
            if (seen.size > 500) seen.clear()

            _incoming.tryEmit(msg)

            // Relay onward (multi-hop)
            if (msg.ttl > 1) {
                val fwd = msg.copy(ttl = msg.ttl - 1, path = msg.path + myName)
                peerNames.keys.filter { it != endpointId }.forEach { sendTo(it, fwd) }
            }
        }

        override fun onPayloadTransferUpdate(endpointId: String, update: PayloadTransferUpdate) {}
    }

    companion object {
        /** Request these at runtime before calling start(). */
        fun requiredPermissions(): List<String> = buildList {
            add(Manifest.permission.ACCESS_FINE_LOCATION)
            if (Build.VERSION.SDK_INT >= 31) {
                add(Manifest.permission.BLUETOOTH_ADVERTISE)
                add(Manifest.permission.BLUETOOTH_CONNECT)
                add(Manifest.permission.BLUETOOTH_SCAN)
            }
            if (Build.VERSION.SDK_INT >= 33) add(Manifest.permission.NEARBY_WIFI_DEVICES)
        }
    }
}
