import 'package:device_info_plus/device_info_plus.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:fantasma_flutter/src/metadata.dart';

void main() {
  group('device classification', () {
    test('classifies iPad hardware as tablet', () {
      final info = IosDeviceInfo.fromMap(<String, dynamic>{
        'name': 'iPad',
        'systemName': 'iOS',
        'systemVersion': '18.0',
        'model': 'iPad',
        'modelName': 'iPad Pro',
        'localizedModel': 'iPad',
        'identifierForVendor': 'vendor',
        'isPhysicalDevice': true,
        'isiOSAppOnMac': false,
        'physicalRamSize': 8192,
        'availableRamSize': 4096,
        'freeDiskSize': 4096,
        'totalDiskSize': 8192,
        'utsname': <String, dynamic>{
          'sysname': 'Darwin',
          'nodename': 'iPad',
          'release': '23.0.0',
          'version': 'Version',
          'machine': 'iPad14,3',
        },
      });

      expect(classifyIosDeviceFormFactor(info), 'tablet');
    });

    test('classifies iPhone hardware as phone', () {
      final info = IosDeviceInfo.fromMap(<String, dynamic>{
        'name': 'iPhone',
        'systemName': 'iOS',
        'systemVersion': '18.0',
        'model': 'iPhone',
        'modelName': 'iPhone 16',
        'localizedModel': 'iPhone',
        'identifierForVendor': 'vendor',
        'isPhysicalDevice': true,
        'isiOSAppOnMac': false,
        'physicalRamSize': 8192,
        'availableRamSize': 4096,
        'freeDiskSize': 4096,
        'totalDiskSize': 8192,
        'utsname': <String, dynamic>{
          'sysname': 'Darwin',
          'nodename': 'iPhone',
          'release': '23.0.0',
          'version': 'Version',
          'machine': 'iPhone16,1',
        },
      });

      expect(classifyIosDeviceFormFactor(info), 'phone');
    });

    test('classifies android tablets from stable device hints', () {
      final info = AndroidDeviceInfo.fromMap(<String, dynamic>{
        'version': <String, dynamic>{
          'baseOS': '',
          'codename': 'REL',
          'incremental': '1',
          'previewSdkInt': 0,
          'release': '16',
          'sdkInt': 35,
          'securityPatch': '2026-03-01',
        },
        'board': 'board',
        'bootloader': 'boot',
        'brand': 'google',
        'device': 'tablet',
        'display': 'display',
        'fingerprint': 'fingerprint',
        'hardware': 'hardware',
        'host': 'host',
        'id': 'id',
        'manufacturer': 'Google',
        'model': 'Pixel Tablet',
        'product': 'tablet',
        'name': 'Pixel Tablet',
        'supported32BitAbis': <String>[],
        'supported64BitAbis': <String>['arm64-v8a'],
        'supportedAbis': <String>['arm64-v8a'],
        'tags': 'release',
        'type': 'user',
        'isPhysicalDevice': true,
        'freeDiskSize': 4096,
        'totalDiskSize': 8192,
        'systemFeatures': <String>['android.hardware.touchscreen'],
        'serialNumber': 'serial',
        'isLowRamDevice': false,
        'physicalRamSize': 8192,
        'availableRamSize': 4096,
      });

      expect(classifyAndroidDeviceFormFactor(info), 'tablet');
    });

    test('classifies android phones from telephony capability', () {
      final info = AndroidDeviceInfo.fromMap(<String, dynamic>{
        'version': <String, dynamic>{
          'baseOS': '',
          'codename': 'REL',
          'incremental': '1',
          'previewSdkInt': 0,
          'release': '16',
          'sdkInt': 35,
          'securityPatch': '2026-03-01',
        },
        'board': 'board',
        'bootloader': 'boot',
        'brand': 'google',
        'device': 'phone',
        'display': 'display',
        'fingerprint': 'fingerprint',
        'hardware': 'hardware',
        'host': 'host',
        'id': 'id',
        'manufacturer': 'Google',
        'model': 'Pixel 10',
        'product': 'phone',
        'name': 'Pixel 10',
        'supported32BitAbis': <String>[],
        'supported64BitAbis': <String>['arm64-v8a'],
        'supportedAbis': <String>['arm64-v8a'],
        'tags': 'release',
        'type': 'user',
        'isPhysicalDevice': true,
        'freeDiskSize': 4096,
        'totalDiskSize': 8192,
        'systemFeatures': <String>[
          'android.hardware.telephony',
          'android.hardware.touchscreen',
        ],
        'serialNumber': 'serial',
        'isLowRamDevice': false,
        'physicalRamSize': 8192,
        'availableRamSize': 4096,
      });

      expect(classifyAndroidDeviceFormFactor(info), 'phone');
    });

    test('falls back to unknown when android hints are missing', () {
      final info = AndroidDeviceInfo.fromMap(<String, dynamic>{
        'version': <String, dynamic>{
          'baseOS': '',
          'codename': 'REL',
          'incremental': '1',
          'previewSdkInt': 0,
          'release': '16',
          'sdkInt': 35,
          'securityPatch': '2026-03-01',
        },
        'board': 'board',
        'bootloader': 'boot',
        'brand': 'google',
        'device': '',
        'display': 'display',
        'fingerprint': 'fingerprint',
        'hardware': 'hardware',
        'host': 'host',
        'id': 'id',
        'manufacturer': 'Google',
        'model': '',
        'product': '',
        'name': '',
        'supported32BitAbis': <String>[],
        'supported64BitAbis': <String>['arm64-v8a'],
        'supportedAbis': <String>['arm64-v8a'],
        'tags': 'release',
        'type': 'user',
        'isPhysicalDevice': true,
        'freeDiskSize': 4096,
        'totalDiskSize': 8192,
        'systemFeatures': <String>[],
        'serialNumber': 'serial',
        'isLowRamDevice': false,
        'physicalRamSize': 8192,
        'availableRamSize': 4096,
      });

      expect(classifyAndroidDeviceFormFactor(info), 'unknown');
    });
  });
}
