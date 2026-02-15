using System;

namespace Qado.Logging
{
    public enum LogLevel : byte
    {
        Info = 1,
        Warn = 2,
        Error = 3
    }

    public interface ILogSink
    {
        void Info(string category, string message);
        void Warn(string category, string message);
        void Error(string category, string message);


        void Log(LogLevel level, string category, string message)
        {
            category ??= "";
            message ??= "";

            switch (level)
            {
                case LogLevel.Info: Info(category, message); break;
                case LogLevel.Warn: Warn(category, message); break;
                default: Error(category, message); break;
            }
        }

        void Error(string category, Exception ex)
            => Error(category, ex?.ToString() ?? "Exception is null");

        void Error(string category, string message, Exception ex)
            => Error(category, $"{message} ({ex.GetType().Name}: {ex.Message})");
    }
}

