import java.util.Properties

plugins {
    id("com.android.application")
    id("kotlin-android")
    // The Flutter Gradle Plugin must be applied after the Android and Kotlin Gradle plugins.
    id("dev.flutter.flutter-gradle-plugin")
}

// Worktree-aware debug identity (gitignored, written by
// scripts/mobile-worktree-overrides.sh): debug builds from a git worktree get a
// branch-labelled app name and a unique applicationId suffix so builds from
// multiple worktrees install side by side. Release builds never read this.
val worktreePropsFile = rootProject.file("worktree.properties")
val worktreeProps =
    Properties().apply {
        if (worktreePropsFile.isFile) worktreePropsFile.inputStream().use { load(it) }
    }
val worktreeLabel = worktreeProps.getProperty("label")?.takeIf { it.isNotBlank() }
if (worktreeLabel != null && !worktreeLabel.matches(Regex("""[A-Za-z0-9._-]+"""))) {
    throw GradleException(
        "worktree.properties label must match [A-Za-z0-9._-]+ (safe for string " +
            "resources), got: " + worktreeLabel,
    )
}
val worktreeIdSuffix =
    worktreeProps.getProperty("applicationIdSuffix")?.takeIf { it.isNotBlank() }
if (worktreeIdSuffix != null && !worktreeIdSuffix.matches(Regex("""\.[a-z][a-z0-9_]*"""))) {
    throw GradleException(
        "worktree.properties applicationIdSuffix must match \\.[a-z][a-z0-9_]*, got: " +
            worktreeIdSuffix,
    )
}

android {
    namespace = "xyz.block.buzz.mobile"
    compileSdk = flutter.compileSdkVersion
    ndkVersion = flutter.ndkVersion

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions {
        jvmTarget = JavaVersion.VERSION_17.toString()
    }

    defaultConfig {
        applicationId = "xyz.block.buzz.mobile"
        // You can update the following values to match your application needs.
        // For more information, see: https://flutter.dev/to/review-gradle-config.
        minSdk = flutter.minSdkVersion
        targetSdk = flutter.targetSdkVersion
        versionCode = flutter.versionCode
        versionName = flutter.versionName
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        resValue("string", "app_name", "Buzz")
    }

    buildTypes {
        debug {
            // Only debug builds take the worktree identity; release/profile
            // keep the production applicationId and label.
            if (worktreeIdSuffix != null) {
                applicationIdSuffix = worktreeIdSuffix
            }
            if (worktreeLabel != null) {
                resValue("string", "app_name", "Buzz ($worktreeLabel)")
            }
        }
        release {
            // Local release APKs are installable test artifacts. Distribution
            // signing is applied separately when publishing to Zapstore.
            signingConfig = signingConfigs.getByName("debug")
        }
    }
}

dependencies {
    testImplementation(kotlin("test"))

    androidTestImplementation(kotlin("test"))
    androidTestImplementation("androidx.test.ext:junit:1.3.0")
    androidTestImplementation("androidx.test:runner:1.7.0")
}

flutter {
    source = "../.."
}

// With `-PbuzzFipsMobile`, cargo-ndk packages the Android FFI library as jniLibs.
val buzzFipsMobileEnabled = providers.gradleProperty("buzzFipsMobile").isPresent
if (buzzFipsMobileEnabled) {
    val workspaceRoot = rootProject.projectDir.parentFile.parentFile
    val bridgeManifest = workspaceRoot.resolve("crates/buzz-fips-mobile/Cargo.toml")
    val jniLibs = project.layout.projectDirectory.dir("src/main/jniLibs")
    val androidAbis = listOf("arm64-v8a", "armeabi-v7a", "x86_64")

    val buildBuzzFipsMobile =
        tasks.register<Exec>("buildBuzzFipsMobile") {
            workingDir = workspaceRoot
            commandLine(
                "cargo",
                "ndk",
                *androidAbis.flatMap { listOf("-t", it) }.toTypedArray(),
                "-o",
                jniLibs.asFile.absolutePath,
                "build",
                "--manifest-path",
                bridgeManifest.absolutePath,
                "--release",
            )
        }

    tasks.matching { it.name in setOf("assembleDebug", "assembleRelease", "bundleRelease") }.configureEach {
        dependsOn(buildBuzzFipsMobile)
    }
}
