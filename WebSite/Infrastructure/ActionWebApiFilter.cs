using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http.Controllers;
using System.Web.Http.Filters;

namespace Server.Infrastructure
{
    public class ActionWebApiFilter : ActionFilterAttribute
    {
        ThreadLocal<Stopwatch> Timer = new ThreadLocal<Stopwatch>(() => Stopwatch.StartNew());

        public override void OnActionExecuting(HttpActionContext actionContext)
        {
            // pre-processing
            //Trace.WriteLine("Starting Request: " + actionContext.Request.RequestUri.ToString());
            Timer.Value.Restart();
        }

        public async override void OnActionExecuted(HttpActionExecutedContext actionExecutedContext)
        {
            //var objectContent = actionExecutedContext.Response.Content as ObjectContent;
            //if (objectContent != null)
            //{
            //    var type = objectContent.ObjectType; //type of the returned object
            //    var value = objectContent.Value; //holding the returned value
            //}

            Trace.WriteLine(string.Format("Request:   {0}", actionExecutedContext.Request.RequestUri.ToString()));
            Trace.WriteLine(string.Format("    Took {0} ({1:N2} msecs)", Timer.Value.Elapsed, Timer.Value.Elapsed.TotalMilliseconds));
            var response = await actionExecutedContext.Response.Content.ReadAsStringAsync();
            var headers = actionExecutedContext.Response.Content.Headers;
            Trace.WriteLine(string.Format("    Headers: {0}", string.Join(", ", headers.Select(h => new { h.Key, Values = String.Join(", ", h.Value) }))));

            var fileName = "Response-" + Guid.NewGuid() + ".json";
            var dataFolder = HttpContext.Current.Server.MapPath("~/Data");
            Trace.WriteLine(string.Format("    Contents saved as {0}", fileName));
            try
            {
                dynamic parsedJson = JsonConvert.DeserializeObject(response);
                var formattedJson = JsonConvert.SerializeObject(parsedJson, Formatting.Indented);
                File.WriteAllText(Path.Combine(dataFolder, fileName), formattedJson);
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex);
                File.WriteAllText(Path.Combine(dataFolder, fileName), response);
            }
        }
    }
}