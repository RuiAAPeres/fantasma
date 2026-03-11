// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "FantasmaSDK",
    platforms: [
        .iOS(.v17),
        .macOS(.v14),
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
            path: "Sources/CSQLite"
        ),
        .target(
            name: "FantasmaSDK",
            dependencies: ["CSQLite"]
        ),
        .testTarget(
            name: "FantasmaSDKTests",
            dependencies: ["FantasmaSDK"]
        ),
    ]
)
