using System;
using System.Globalization;

namespace Qado.CodeBehindHelper
{
    public static class QadoAmountParser
    {
        private const int Scale = 9;

        private const int MaxInputLength = 64;

        private const ulong Pow10Scale = 1_000_000_000UL; // atoms per QADO

        public static bool TryParseQadoToNanoU64(string input, out ulong nanoAmount)
            => TryParseQadoToAtomsU64(input, out nanoAmount);

        public static string FormatNanoToQado(ulong nanoAmount)
            => FormatAtomsToQado(nanoAmount);

        public static bool TryParseQadoToAtomsU64(string input, out ulong atoms)
        {
            atoms = 0;
            if (string.IsNullOrWhiteSpace(input)) return false;

            var s = input.Trim();

            if (s.Length > MaxInputLength) return false;

            s = s.Replace("_", "").Replace(" ", "");

            if (s.StartsWith("-", StringComparison.Ordinal) || s.StartsWith("+", StringComparison.Ordinal)) return false;
            if (s.IndexOf('e') >= 0 || s.IndexOf('E') >= 0) return false;

            int lastDot = s.LastIndexOf('.');
            int lastComma = s.LastIndexOf(',');

            char decimalSep = '\0';
            char groupingSep = '\0';

            if (lastDot >= 0 && lastComma >= 0)
            {
                if (lastDot > lastComma)
                {
                    decimalSep = '.';
                    groupingSep = ',';
                }
                else
                {
                    decimalSep = ',';
                    groupingSep = '.';
                }
            }
            else if (lastDot >= 0)
            {
                decimalSep = '.';
            }
            else if (lastComma >= 0)
            {
                decimalSep = ',';
            }

            if (groupingSep != '\0')
                s = s.Replace(groupingSep.ToString(), "");

            if (decimalSep == ',')
                s = s.Replace(',', '.'); // normalize decimal separator

            if (s == ".") return false;
            if (s.StartsWith(".", StringComparison.Ordinal)) s = "0" + s;
            if (s.EndsWith(".", StringComparison.Ordinal)) s += "0";

            string intPart;
            string fracPart;

            int dot = s.IndexOf('.');
            if (dot < 0)
            {
                intPart = s;
                fracPart = "";
            }
            else
            {
                if (s.IndexOf('.', dot + 1) >= 0) return false;

                intPart = s[..dot];
                fracPart = s[(dot + 1)..];
            }

            if (intPart.Length == 0) intPart = "0";

            if (!AllDigits(intPart)) return false;
            if (fracPart.Length > 0 && !AllDigits(fracPart)) return false;

            if (fracPart.Length > Scale) return false;

            if (!ulong.TryParse(intPart, NumberStyles.None, CultureInfo.InvariantCulture, out var whole))
                return false;

            ulong wholeAtoms;
            if (Scale == 9)
            {
                if (whole > ulong.MaxValue / Pow10Scale) return false;
                wholeAtoms = whole * Pow10Scale;
            }
            else
            {
                if (!TryMulPow10U64(whole, Scale, out wholeAtoms)) return false;
            }

            ulong fracAtoms = 0;
            if (fracPart.Length > 0)
            {
                string fracPadded = fracPart.PadRight(Scale, '0');
                if (!ulong.TryParse(fracPadded, NumberStyles.None, CultureInfo.InvariantCulture, out fracAtoms))
                    return false;
            }

            if (ulong.MaxValue - wholeAtoms < fracAtoms)
                return false;

            atoms = wholeAtoms + fracAtoms;
            return true;
        }

        public static string FormatAtomsToQado(ulong atoms)
        {
            if (atoms == 0) return "0";
            if (Scale == 0) return atoms.ToString(CultureInfo.InvariantCulture);

            var n = atoms.ToString(CultureInfo.InvariantCulture);

            if (n.Length <= Scale)
            {
                int pad = Scale - n.Length;
                string frac = new string('0', pad) + n;
                frac = frac.TrimEnd('0');
                return frac.Length == 0 ? "0" : "0." + frac;
            }

            string intPart = n[..(n.Length - Scale)];
            string fracPart = n[(n.Length - Scale)..];

            fracPart = fracPart.TrimEnd('0');
            return fracPart.Length == 0 ? intPart : $"{intPart}.{fracPart}";
        }

        private static bool AllDigits(string s)
        {
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (c < '0' || c > '9') return false;
            }
            return true;
        }

        private static bool TryMulPow10U64(ulong v, int p, out ulong result)
        {
            result = v;
            for (int i = 0; i < p; i++)
            {
                if (result > ulong.MaxValue / 10UL)
                {
                    result = 0;
                    return false;
                }
                result *= 10UL;
            }
            return true;
        }
    }
}

