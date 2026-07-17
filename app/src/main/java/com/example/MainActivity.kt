package com.example

import android.app.Application
import android.content.Context
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.Send
import androidx.compose.material.icons.filled.Info
import androidx.compose.material.icons.filled.Clear
import androidx.compose.material.icons.filled.Search
import androidx.compose.material.icons.filled.Computer
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import com.example.ui.theme.MyApplicationTheme
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONObject
import java.net.Inet4Address
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.Socket
import java.util.concurrent.TimeUnit

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContent {
            MyApplicationTheme {
                TyperScreen()
            }
        }
    }
}

class TyperViewModel(application: Application) : AndroidViewModel(application) {
    private val prefs = application.getSharedPreferences("TyperPrefs", Context.MODE_PRIVATE)

    private val client = OkHttpClient.Builder()
        .connectTimeout(5, TimeUnit.SECONDS)
        .writeTimeout(5, TimeUnit.SECONDS)
        .readTimeout(5, TimeUnit.SECONDS)
        .build()

    private var webSocket: WebSocket? = null

    private val _ipAddress = MutableStateFlow(prefs.getString("server_ip", "") ?: "")
    val ipAddress = _ipAddress.asStateFlow()

    private val _textInput = MutableStateFlow("")
    val textInput = _textInput.asStateFlow()

    private val _serverName = MutableStateFlow("")
    val serverName = _serverName.asStateFlow()

    private val _statusMessage = MutableStateFlow("Disconnected")
    val statusMessage = _statusMessage.asStateFlow()

    private val _isConnected = MutableStateFlow(false)
    val isConnected = _isConnected.asStateFlow()

    fun updateIpAddress(ip: String) {
        _ipAddress.value = ip
        prefs.edit().putString("server_ip", ip).apply()
    }

    private fun getLocalIpv4Address(): String? {
        try {
            val interfaces = NetworkInterface.getNetworkInterfaces().toList()
            
            // 1. Prefer WiFi interfaces
            for (intf in interfaces) {
                if (intf.name.startsWith("wlan") || intf.name.startsWith("wifi")) {
                    for (addr in intf.inetAddresses) {
                        if (!addr.isLoopbackAddress && addr is Inet4Address) {
                            return addr.hostAddress
                        }
                    }
                }
            }
            
            // 2. Fallback to 192.168.x.x or 10.x.x.x (avoiding cellular rmnet)
            for (intf in interfaces) {
                if (intf.name.startsWith("rmnet")) continue
                for (addr in intf.inetAddresses) {
                    if (!addr.isLoopbackAddress && addr is Inet4Address) {
                        val ip = addr.hostAddress
                        if (ip != null && (ip.startsWith("192.168.") || ip.startsWith("10."))) {
                            return ip
                        }
                    }
                }
            }
            
            // 3. Final fallback
            for (intf in interfaces) {
                for (addr in intf.inetAddresses) {
                    if (!addr.isLoopbackAddress && addr is Inet4Address) {
                        return addr.hostAddress
                    }
                }
            }
        } catch (ex: Exception) {
            ex.printStackTrace()
        }
        return null
    }

    fun scanNetwork() {
        viewModelScope.launch(Dispatchers.IO) {
            val localIp = getLocalIpv4Address()
            if (localIp == null) {
                _statusMessage.value = "Could not determine local IP. Are you on Wi-Fi?"
                return@launch
            }
            
            val prefix = localIp.substringBeforeLast(".")
            _statusMessage.value = "Starting scan on $prefix.*"

            val scanClient = OkHttpClient.Builder()
                .connectTimeout(2000, TimeUnit.MILLISECONDS)
                .readTimeout(2000, TimeUnit.MILLISECONDS)
                .build()

            var foundIp: String? = null
            
            val ipRange = (1..254).toList()
            val batches = ipRange.chunked(40) // Chunk size of 40 to prevent thread starvation
            
            for (batch in batches) {
                if (foundIp != null) break
                
                _statusMessage.value = "Scanning $prefix.${batch.first()} to ${batch.last()}..."
                
                val results = coroutineScope {
                    batch.map { i ->
                        async {
                            val testIp = "$prefix.$i"
                            val url = "http://$testIp:8766/"
                            val request = Request.Builder().url(url).build()
                            try {
                                scanClient.newCall(request).execute().use { response ->
                                    if (response.isSuccessful) {
                                        val body = response.body?.string()
                                        if (body != null && body.contains("hostname")) {
                                            testIp
                                        } else null
                                    } else null
                                }
                            } catch (e: Exception) {
                                null
                            }
                        }
                    }.awaitAll()
                }
                
                foundIp = results.firstOrNull { it != null }
            }
            
            if (foundIp != null) {
                val newIp = "$foundIp:8765"
                updateIpAddress(newIp)
                _statusMessage.value = "Found server at $newIp!"
                connect() // Auto-connect
            } else {
                _statusMessage.value = "No server found on $prefix.*:8766."
            }
        }
    }

    fun connect() {
        val currentIp = ipAddress.value.trim()
        if (currentIp.isBlank()) {
            _statusMessage.value = "Enter IP address"
            return
        }

        val url = if (currentIp.startsWith("ws://")) {
            currentIp
        } else {
            if (currentIp.contains(":")) "ws://$currentIp" else "ws://$currentIp:8765"
        }

        val request = Request.Builder().url(url).build()
        webSocket?.cancel()

        _statusMessage.value = "Connecting..."
        webSocket = client.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: okhttp3.Response) {
                _isConnected.value = true
                _statusMessage.value = "Connected to $url"
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                try {
                    val json = JSONObject(text)
                    if (json.optString("action") == "server_info") {
                        _serverName.value = json.optString("hostname")
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                _isConnected.value = false
                _serverName.value = ""
                _statusMessage.value = "Disconnected"
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: okhttp3.Response?) {
                _isConnected.value = false
                _serverName.value = ""
                _statusMessage.value = "Connection failed: ${t.localizedMessage}"
            }
        })
    }

    fun disconnect() {
        webSocket?.close(1000, "User disconnected")
        webSocket = null
        _isConnected.value = false
        _serverName.value = ""
        _statusMessage.value = "Disconnected"
    }

    private val _voiceInput = MutableStateFlow("")
    val voiceInput = _voiceInput.asStateFlow()

    fun updateVoiceInput(newText: String) {
        _voiceInput.value = newText
    }

    fun sendVoiceDictation() {
        if (!_isConnected.value || webSocket == null) {
            _statusMessage.value = "Please connect first."
            return
        }
        val text = _voiceInput.value
        if (text.isNotEmpty()) {
            webSocket?.send(JSONObject().apply {
                put("action", "type_text")
                put("text", text)
            }.toString())
            
            webSocket?.send(JSONObject().apply {
                put("action", "type_char")
                put("char", " ")
            }.toString())
            _voiceInput.value = ""
        }
    }

    fun updateTextInput(newText: String) {
        val oldText = _textInput.value
        _textInput.value = newText

        if (!_isConnected.value || webSocket == null) {
            _statusMessage.value = "Please connect first."
            return
        }

        val commonPrefixLength = newText.commonPrefixWith(oldText).length
        val backspaces = oldText.length - commonPrefixLength
        val textToSend = newText.substring(commonPrefixLength)

        for (i in 0 until backspaces) {
            webSocket?.send(JSONObject().apply {
                put("action", "backspace")
            }.toString())
        }

        if (textToSend.length == 1) {
            webSocket?.send(JSONObject().apply {
                put("action", "type_char")
                put("char", textToSend)
            }.toString())
        } else if (textToSend.length > 1) {
            webSocket?.send(JSONObject().apply {
                put("action", "type_text")
                put("text", textToSend)
            }.toString())
        }
    }

    fun clearLocalText() {
        _textInput.value = ""
        // Do not send anything to laptop
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun TyperScreen(viewModel: TyperViewModel = viewModel()) {
    val ipAddress by viewModel.ipAddress.collectAsState()
    val textInput by viewModel.textInput.collectAsState()
    val statusMessage by viewModel.statusMessage.collectAsState()
    val isConnected by viewModel.isConnected.collectAsState()
    val serverName by viewModel.serverName.collectAsState()
    val voiceInput by viewModel.voiceInput.collectAsState()

    var showInstructions by remember { mutableStateOf(false) }
    var selectedTabIndex by remember { mutableStateOf(0) }
    val tabs = listOf("Keyboard Mode", "Voice Mode")
    val focusManager = androidx.compose.ui.platform.LocalFocusManager.current

    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("Remote Typer") },
                actions = {
                    IconButton(onClick = { showInstructions = true }) {
                        Icon(Icons.Default.Info, contentDescription = "Instructions")
                    }
                },
                colors = TopAppBarDefaults.topAppBarColors(
                    containerColor = MaterialTheme.colorScheme.primaryContainer,
                    titleContentColor = MaterialTheme.colorScheme.onPrimaryContainer
                )
            )
        }
    ) { innerPadding ->
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(innerPadding)
                .padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            if (isConnected) {
                Card(
                    modifier = Modifier.fillMaxWidth(),
                    colors = CardDefaults.cardColors(
                        containerColor = MaterialTheme.colorScheme.secondaryContainer
                    )
                ) {
                    Row(
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(16.dp),
                        verticalAlignment = Alignment.CenterVertically,
                        horizontalArrangement = Arrangement.SpaceBetween
                    ) {
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            Icon(
                                Icons.Default.Computer,
                                contentDescription = "Computer",
                                tint = MaterialTheme.colorScheme.onSecondaryContainer,
                                modifier = Modifier.size(32.dp)
                            )
                            Spacer(modifier = Modifier.width(16.dp))
                            Column {
                                Text(
                                    text = if (serverName.isNotEmpty()) serverName else "Connected Device",
                                    style = MaterialTheme.typography.titleMedium,
                                    color = MaterialTheme.colorScheme.onSecondaryContainer
                                )
                                Text(
                                    text = ipAddress,
                                    style = MaterialTheme.typography.bodySmall,
                                    color = MaterialTheme.colorScheme.onSecondaryContainer.copy(alpha = 0.7f)
                                )
                            }
                        }
                        Button(onClick = viewModel::disconnect) {
                            Text("Disconnect")
                        }
                    }
                }
            } else {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    OutlinedTextField(
                        value = ipAddress,
                        onValueChange = viewModel::updateIpAddress,
                        label = { Text("Server IP Address & Port") },
                        placeholder = { Text("e.g. 192.168.1.100:8765") },
                        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Uri),
                        modifier = Modifier.weight(1f),
                        singleLine = true,
                        trailingIcon = {
                            IconButton(onClick = viewModel::scanNetwork) {
                                Icon(Icons.Default.Search, contentDescription = "Scan Network")
                            }
                        }
                    )
                    Button(
                        onClick = viewModel::connect,
                        modifier = Modifier.padding(top = 8.dp)
                    ) {
                        Text("Connect")
                    }
                }
            }

            TabRow(selectedTabIndex = selectedTabIndex) {
                tabs.forEachIndexed { index, title ->
                    Tab(
                        text = { Text(title) },
                        selected = selectedTabIndex == index,
                        onClick = { selectedTabIndex = index }
                    )
                }
            }

            if (selectedTabIndex == 0) {
                OutlinedTextField(
                    value = textInput,
                    onValueChange = viewModel::updateTextInput,
                    label = { Text("Type here (sent instantly to laptop)") },
                    modifier = Modifier
                        .fillMaxWidth()
                        .weight(1f),
                    maxLines = 10
                )

                Button(
                    onClick = viewModel::clearLocalText,
                    modifier = Modifier
                        .fillMaxWidth()
                        .height(56.dp),
                    colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.secondary)
                ) {
                    Icon(Icons.Default.Clear, contentDescription = null)
                    Spacer(modifier = Modifier.width(8.dp))
                    Text("Clear Local Text (Won't delete from laptop)")
                }
            } else {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.End
                ) {
                    Button(
                        onClick = { 
                            viewModel.sendVoiceDictation()
                            focusManager.clearFocus()
                        },
                        modifier = Modifier.height(56.dp),
                        colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.primary)
                    ) {
                        Icon(Icons.AutoMirrored.Filled.Send, contentDescription = "Send Dictation")
                        Spacer(modifier = Modifier.width(8.dp))
                        Text("Send")
                    }
                }

                OutlinedTextField(
                    value = voiceInput,
                    onValueChange = viewModel::updateVoiceInput,
                    label = { Text("Voice Dictation") },
                    placeholder = { Text("Tap here, then press the microphone icon on your keyboard to speak...") },
                    modifier = Modifier
                        .fillMaxWidth()
                        .weight(1f),
                    maxLines = 15
                )
            }

            if (statusMessage.isNotEmpty()) {
                Text(
                    text = statusMessage,
                    color = if (statusMessage.startsWith("Failed") || statusMessage.startsWith("Error")) 
                        MaterialTheme.colorScheme.error 
                    else 
                        MaterialTheme.colorScheme.primary,
                    style = MaterialTheme.typography.bodyMedium
                )
            }
        }
    }

    if (showInstructions) {
        AlertDialog(
            onDismissRequest = { showInstructions = false },
            title = { Text("Python Server Setup") },
            text = {
                Column(modifier = Modifier.verticalScroll(rememberScrollState())) {
                    Text("To use this app, run a Python server on your laptop. Both devices must be on the same Wi-Fi network.")
                    Spacer(modifier = Modifier.height(16.dp))
                    Text("1. Install dependencies:", style = MaterialTheme.typography.labelLarge)
                    Surface(color = MaterialTheme.colorScheme.surfaceVariant, shape = MaterialTheme.shapes.small) {
                        Text("pip install websockets pyautogui", modifier = Modifier.padding(8.dp), fontFamily = FontFamily.Monospace, fontSize = MaterialTheme.typography.bodySmall.fontSize)
                    }
                    Spacer(modifier = Modifier.height(16.dp))
                    Text("2. Create server.py:", style = MaterialTheme.typography.labelLarge)
                    Surface(color = MaterialTheme.colorScheme.surfaceVariant, shape = MaterialTheme.shapes.small) {
                        Text(
                            """
import asyncio
import websockets
import json
import pyautogui
import socket

async def handler(websocket):
    print("Device connected!")
    hostname = socket.gethostname()
    await websocket.send(json.dumps({"action": "server_info", "hostname": hostname}))
    async for message in websocket:
        try:
            data = json.loads(message)
            action = data.get("action")
            if action == "type_char":
                char = data.get("char", "")
                if char:
                    pyautogui.write(char)
            elif action == "type_text":
                text = data.get("text", "")
                if text:
                    pyautogui.write(text)
            elif action == "backspace":
                pyautogui.press("backspace")
        except Exception as e:
            print(f"Error: {e}")

async def main():
    print("WebSocket server running on ws://0.0.0.0:5000")
    async with websockets.serve(handler, "0.0.0.0", 5000):
        await asyncio.Future()  # run forever

if __name__ == '__main__':
    asyncio.run(main())
                            """.trimIndent(),
                            modifier = Modifier.padding(8.dp),
                            fontFamily = FontFamily.Monospace,
                            fontSize = MaterialTheme.typography.bodySmall.fontSize
                        )
                    }
                    Spacer(modifier = Modifier.height(16.dp))
                    Text("3. Run the script:", style = MaterialTheme.typography.labelLarge)
                    Surface(color = MaterialTheme.colorScheme.surfaceVariant, shape = MaterialTheme.shapes.small) {
                        Text("python server.py", modifier = Modifier.padding(8.dp), fontFamily = FontFamily.Monospace, fontSize = MaterialTheme.typography.bodySmall.fontSize)
                    }
                    Spacer(modifier = Modifier.height(16.dp))
                    Text("Finally, enter your laptop's local IP address (e.g., 192.168.1.5:8765) in the app.")
                }
            },
            confirmButton = {
                TextButton(onClick = { showInstructions = false }) {
                    Text("Got it")
                }
            }
        )
    }
}
