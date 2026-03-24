import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id("com.android.library") version "8.5.2"
    kotlin("android") version "2.0.21"
    kotlin("kapt") version "2.0.21"
    kotlin("plugin.serialization") version "2.0.21"
    id("androidx.room") version "2.6.1"
}

val reactNativeVersion = providers.gradleProperty("fantasmaReactNativeVersion").orElse("0.84.1")

android {
    namespace = "com.fantasma.reactnative"
    compileSdk = 35

    defaultConfig {
        minSdk = 26
        consumerProguardFiles("consumer-rules.pro")
    }

    buildFeatures {
        buildConfig = false
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        freeCompilerArgs.add("-Xexplicit-api=strict")
    }
}

room {
    schemaDirectory("$projectDir/schemas")
}

dependencies {
    compileOnly("com.facebook.react:react-android:${reactNativeVersion.get()}")

    implementation("androidx.core:core-ktx:1.13.1")
    implementation("androidx.lifecycle:lifecycle-process:2.8.7")
    implementation("androidx.datastore:datastore-preferences:1.1.1")
    implementation("androidx.room:room-runtime:2.6.1")
    implementation("androidx.room:room-ktx:2.6.1")
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.8.1")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")

    kapt("androidx.room:room-compiler:2.6.1")
}
