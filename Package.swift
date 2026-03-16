// swift-tools-version: 5.6

import PackageDescription

let package = Package(
    name: "FantasmaSDK",
    platforms: [
        .iOS("17.0"),
        .macOS("14.0"),
    ],
    products: [
        .library(
            name: "FantasmaSDK",
            targets: ["FantasmaSDK"]
        ),
    ],
    targets: [
        .systemLibrary(
            name: "CSQLite",
            path: "sdks/ios/FantasmaSDK/Sources/CSQLite"
        ),
        .target(
            name: "FantasmaSDK",
            dependencies: ["CSQLite"],
            path: "sdks/ios/FantasmaSDK/Sources/FantasmaSDK"
        ),
        .testTarget(
            name: "FantasmaSDKTests",
            dependencies: ["FantasmaSDK"],
            path: "sdks/ios/FantasmaSDK/Tests/FantasmaSDKTests"
        ),
    ]
)
