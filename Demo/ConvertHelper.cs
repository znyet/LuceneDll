using System;
using System.ComponentModel;

namespace TestLucene
{
    public static class ConvertHelper
    {
        /// <summary>
        /// string转其他类型
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static T ConvertStringTo<T>(string value)
        {
            if (string.IsNullOrEmpty(value))
                return default(T);
            else
                return (T)HackType(value, typeof(T));
        }

        /// <summary>
        /// 其它各类型互相转换
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static T ConvertObjectTo<T>(object value)
        {
            if (value == null)
                return default(T);
            return (T)HackType(value, typeof(T));
        }


        public static object HackType(object value, Type convertsionType)
        {
            Type valueType = value.GetType();
            if (valueType == convertsionType) //转换类型相等直接返回
            {
                return value;
            }
            else
            {
                if (convertsionType == typeof(string)) //直接返回tostring
                    return value.ToString();

                //判断convertsionType类型是否为泛型，因为nullable是泛型类,
                if (convertsionType.IsGenericType && convertsionType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
                {
                    //如果convertsionType为nullable类，声明一个NullableConverter类，该类提供从Nullable类到基础基元类型的转换
                    NullableConverter nullableConverter = new NullableConverter(convertsionType);
                    //将convertsionType转换为nullable对的基础基元类型
                    convertsionType = nullableConverter.UnderlyingType;
                }
                return Convert.ChangeType(value, convertsionType);
            }

        }



    }
}
