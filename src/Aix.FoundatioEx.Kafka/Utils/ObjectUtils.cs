using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace Aix.FoundatioEx.Kafka.Utils
{
    public class ObjectUtils
    {
        /// <summary>
        /// 把obj的属性 填充到target上
        /// </summary>
        /// <param name="target"></param>
        /// <param name="obj"></param>
        public static void Assign(object target, object obj)
        {
            if (target == null || obj == null) return;

            var pros= obj.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.GetProperty);
            var targetType = target.GetType();
            foreach (var item in pros)
            {
             var temp=   targetType.GetProperty(item.Name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.IgnoreCase);
                if (temp != null)
                {
                    temp.SetValue(target, item.GetValue(obj));
                }
            } 
        }
    }
}
