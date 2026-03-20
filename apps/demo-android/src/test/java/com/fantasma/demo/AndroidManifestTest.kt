package com.fantasma.demo

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import androidx.test.core.app.ApplicationProvider
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import kotlin.test.Test
import kotlin.test.assertTrue

@RunWith(RobolectricTestRunner::class)
internal class AndroidManifestTest {
    @Test
    internal fun `demo app requests internet permission`() {
        val context = ApplicationProvider.getApplicationContext<Context>()

        @Suppress("DEPRECATION")
        val packageInfo = context.packageManager.getPackageInfo(context.packageName, PackageManager.GET_PERMISSIONS)
        val requestedPermissions = packageInfo.requestedPermissions.orEmpty().toSet()

        assertTrue(Manifest.permission.INTERNET in requestedPermissions)
    }
}
