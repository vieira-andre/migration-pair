using Microsoft.Extensions.Configuration;
using Mycenae.Models;
using System.IO;

namespace Mycenae
{
    public static class Config
    {
        private static readonly IConfigurationRoot Configuration = new ConfigurationBuilder()
                                                                       .SetBasePath(Directory.GetCurrentDirectory())
                                                                       .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                                                                       .Build();

        public static ConfigModel Values => Configuration.Get<ConfigModel>();
    }
}