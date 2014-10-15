using StackOverflowTagServer;
using System;
using System.Threading;
using System.Web.Http;
using System.Web.Mvc;
using System.Web.Optimization;
using System.Web.Routing;

namespace Server
{
    public class WebApiApplication : System.Web.HttpApplication
    {
        internal static Lazy<TagServer> TagServer = new Lazy<TagServer>(() =>
            {
                var filename = @"C:\Users\warma11\Downloads\__Code__\StackOverflowTagServer-master\Questions.bin";
                return StackOverflowTagServer.TagServer.CreateFromFile(filename);
            }, LazyThreadSafetyMode.ExecutionAndPublication);

        protected void Application_Start()
        {
            AreaRegistration.RegisterAllAreas();
            GlobalConfiguration.Configure(WebApiConfig.Register);
            FilterConfig.RegisterGlobalFilters(GlobalFilters.Filters);
            RouteConfig.RegisterRoutes(RouteTable.Routes);
            BundleConfig.RegisterBundles(BundleTable.Bundles);

            // For initialisation to happen here, rather than waiting for the first request
            var temp = WebApiApplication.TagServer.Value;
        }
    }
}
