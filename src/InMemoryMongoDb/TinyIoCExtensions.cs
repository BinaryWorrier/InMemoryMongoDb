using System;
using System.Collections.Generic;
using System.Text;
using TinyIoC;
using System.Linq;
namespace InMemoryMongoDb
{
    public interface ITinyIoCInstaller
    {
        void Install(TinyIoCContainer container);
    }
    internal static class TinyIoCExtensions
    {
        public static void RunInstallers(this TinyIoCContainer container)
        {
            var installers = from t in typeof(ITinyIoCInstaller).Assembly.GetTypes() 
                             where typeof(ITinyIoCInstaller).IsInstanceOfType(t)
                             select t;
            
            foreach(var type in installers)
            {
                var installer = (ITinyIoCInstaller) Activator.CreateInstance(type);
                installer.Install(container);
            }
        }

        private static bool IsBaseType<T>(Type baseType)
        {
            if (baseType == typeof(object))
                return false;

            if (baseType == typeof(T))
                return true;

            return IsBaseType<T>(baseType.BaseType);
        }
    }
}
