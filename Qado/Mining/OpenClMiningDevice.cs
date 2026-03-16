using OpenCL.Net;

namespace Qado.Mining
{
    public sealed class OpenClMiningDevice
    {
        internal OpenClMiningDevice(
            string id,
            int platformIndex,
            int deviceIndex,
            string platformName,
            string deviceName,
            string vendor,
            DeviceType deviceType,
            Platform platformHandle,
            Device deviceHandle)
        {
            Id = id;
            PlatformIndex = platformIndex;
            DeviceIndex = deviceIndex;
            PlatformName = platformName;
            DeviceName = deviceName;
            Vendor = vendor;
            DeviceType = deviceType;
            PlatformHandle = platformHandle;
            DeviceHandle = deviceHandle;
        }

        public string Id { get; }
        public int PlatformIndex { get; }
        public int DeviceIndex { get; }
        public string PlatformName { get; }
        public string DeviceName { get; }
        public string Vendor { get; }
        public DeviceType DeviceType { get; }
        internal Platform PlatformHandle { get; }
        internal Device DeviceHandle { get; }

        public string DisplayName => $"{DeviceName} ({Vendor} | {PlatformName})";

        public override string ToString() => DisplayName;
    }
}
