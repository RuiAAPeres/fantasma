import io.gitlab.arturbosch.detekt.extensions.DetektExtension
import org.jlleitschuh.gradle.ktlint.KtlintExtension

plugins {
    id("com.android.application") version "8.5.2" apply false
    id("com.android.library") version "8.5.2" apply false
    kotlin("android") version "2.0.21" apply false
    kotlin("kapt") version "2.0.21" apply false
    kotlin("plugin.serialization") version "2.0.21" apply false
    id("androidx.room") version "2.6.1" apply false
    id("io.gitlab.arturbosch.detekt") version "1.23.6"
    id("org.jlleitschuh.gradle.ktlint") version "12.1.1"
}

subprojects {
    pluginManager.apply("io.gitlab.arturbosch.detekt")
    pluginManager.apply("org.jlleitschuh.gradle.ktlint")

    extensions.configure<DetektExtension> {
        buildUponDefaultConfig = true
        allRules = false
    }

    extensions.configure<KtlintExtension> {
        android.set(true)
    }
}
