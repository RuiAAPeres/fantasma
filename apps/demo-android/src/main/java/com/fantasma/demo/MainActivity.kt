package com.fantasma.demo

import android.os.Bundle
import android.view.Gravity
import android.widget.Button
import android.widget.LinearLayout
import android.widget.TextView
import androidx.activity.ComponentActivity
import androidx.lifecycle.lifecycleScope
import com.fantasma.sdk.FantasmaClient
import com.fantasma.sdk.FantasmaConfig
import com.fantasma.sdk.FantasmaException
import kotlinx.coroutines.launch

class MainActivity : ComponentActivity() {
    private val fantasma: FantasmaClient by lazy {
        FantasmaClient(
            context = this,
            config =
                FantasmaConfig(
                    serverUrl = "http://10.0.2.2:8081",
                    writeKey = "replace-me",
                ),
        )
    }

    private lateinit var statusView: TextView

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        statusView =
            TextView(this).apply {
                text = "Fantasma Android demo"
                gravity = Gravity.CENTER
                textSize = STATUS_TEXT_SIZE_SP
            }

        val trackButton =
            Button(this).apply {
                text = "Track app_open"
                setOnClickListener {
                    launchSdkCall(
                        successMessage = "Tracked app_open",
                    ) {
                        fantasma.track("app_open")
                    }
                }
            }

        val flushButton =
            Button(this).apply {
                text = "Flush queue"
                setOnClickListener {
                    launchSdkCall(
                        successMessage = "Flush finished",
                    ) {
                        fantasma.flush()
                    }
                }
            }

        val clearButton =
            Button(this).apply {
                text = "Rotate identity"
                setOnClickListener {
                    launchSdkCall(
                        successMessage = "Install identity rotated",
                    ) {
                        fantasma.clear()
                    }
                }
            }

        val container =
            LinearLayout(this).apply {
                orientation = LinearLayout.VERTICAL
                gravity = Gravity.CENTER
                setPadding(
                    CONTAINER_PADDING_PX,
                    CONTAINER_PADDING_PX,
                    CONTAINER_PADDING_PX,
                    CONTAINER_PADDING_PX,
                )
                addView(statusView)
                addView(trackButton)
                addView(flushButton)
                addView(clearButton)
            }

        setContentView(container)
    }

    override fun onDestroy() {
        fantasma.close()
        super.onDestroy()
    }

    private fun launchSdkCall(
        successMessage: String,
        action: suspend () -> Unit,
    ) {
        lifecycleScope.launch {
            statusView.text =
                try {
                    action()
                    successMessage
                } catch (error: FantasmaException) {
                    "Fantasma error: ${error::class.simpleName}"
                }
        }
    }

    private companion object {
        const val CONTAINER_PADDING_PX: Int = 48
        const val STATUS_TEXT_SIZE_SP: Float = 18f
    }
}
