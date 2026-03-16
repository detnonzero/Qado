using System;
using System.Collections.Generic;
using OpenCL.Net;
using Qado.Logging;

namespace Qado.Mining
{
    public static class OpenClDiscovery
    {
        public static IReadOnlyList<OpenClMiningDevice> DiscoverDevices(ILogSink? log = null)
        {
            try
            {
                ErrorCode error;
                var platforms = Cl.GetPlatformIDs(out error);
                if (error != ErrorCode.Success || platforms == null || platforms.Length == 0)
                    return Array.Empty<OpenClMiningDevice>();

                var devices = new List<OpenClMiningDevice>(8);

                for (int platformIndex = 0; platformIndex < platforms.Length; platformIndex++)
                {
                    var platform = platforms[platformIndex];
                    string platformName = SafeInfoString(() => Cl.GetPlatformInfo(platform, PlatformInfo.Name, out _));
                    Device[] rawDevices;

                    try
                    {
                        rawDevices = Cl.GetDeviceIDs(platform, DeviceType.All, out error);
                    }
                    catch
                    {
                        continue;
                    }

                    if (error != ErrorCode.Success || rawDevices == null || rawDevices.Length == 0)
                        continue;

                    for (int deviceIndex = 0; deviceIndex < rawDevices.Length; deviceIndex++)
                    {
                        var device = rawDevices[deviceIndex];
                        var deviceType = SafeDeviceType(device);
                        if (!IsGpuLike(deviceType))
                            continue;

                        string vendor = SafeInfoString(() => Cl.GetDeviceInfo(device, DeviceInfo.Vendor, out _));
                        string deviceName = SafeInfoString(() => Cl.GetDeviceInfo(device, DeviceInfo.Name, out _));
                        string id = BuildDeviceId(platformIndex, deviceIndex, platformName, vendor, deviceName);

                        devices.Add(new OpenClMiningDevice(
                            id: id,
                            platformIndex: platformIndex,
                            deviceIndex: deviceIndex,
                            platformName: platformName,
                            deviceName: deviceName,
                            vendor: vendor,
                            deviceType: deviceType,
                            platformHandle: platform,
                            deviceHandle: device));
                    }
                }

                return devices;
            }
            catch (DllNotFoundException)
            {
                log?.Warn("Mining", "OpenCL runtime not found; OpenCL mining is unavailable.");
                return Array.Empty<OpenClMiningDevice>();
            }
            catch (Exception ex)
            {
                log?.Warn("Mining", $"OpenCL discovery failed: {ex.Message}");
                return Array.Empty<OpenClMiningDevice>();
            }
        }

        public static OpenClMiningDevice? FindById(string? id, ILogSink? log = null)
        {
            if (string.IsNullOrWhiteSpace(id))
                return null;

            var devices = DiscoverDevices(log);
            for (int i = 0; i < devices.Count; i++)
            {
                if (string.Equals(devices[i].Id, id, StringComparison.Ordinal))
                    return devices[i];
            }

            return null;
        }

        private static string BuildDeviceId(int platformIndex, int deviceIndex, string platformName, string vendor, string deviceName)
            => $"{platformIndex}:{deviceIndex}:{Normalize(platformName)}|{Normalize(vendor)}|{Normalize(deviceName)}";

        private static bool IsGpuLike(DeviceType type)
            => (type & DeviceType.Gpu) == DeviceType.Gpu || (type & DeviceType.Accelerator) == DeviceType.Accelerator;

        private static DeviceType SafeDeviceType(Device device)
        {
            try
            {
                ErrorCode error;
                return Cl.GetDeviceInfo(device, DeviceInfo.Type, out error).CastTo<DeviceType>();
            }
            catch
            {
                return 0;
            }
        }

        private static string SafeInfoString(Func<InfoBuffer> getter)
        {
            try
            {
                return getter().ToString().Trim();
            }
            catch
            {
                return string.Empty;
            }
        }

        private static string Normalize(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return string.Empty;

            return value.Trim().ToLowerInvariant();
        }
    }
}
