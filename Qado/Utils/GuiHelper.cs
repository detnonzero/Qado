using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Qado.Utils
{
    public static class GuiUtils
    {
        public static bool IsDesignMode => System.ComponentModel.DesignerProperties.GetIsInDesignMode(new System.Windows.DependencyObject());
    }

}

