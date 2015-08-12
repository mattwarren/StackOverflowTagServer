using Microsoft.ApplicationInsights;
using StackOverflowTagServer;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Web.Http;

namespace Server.Controllers
{
    public class InfoController : ApiController
    {
        [Route("api")]
        [Route("api/Info")]
        [HttpGet]
        public object Info()
        {
            var telemetry = new TelemetryClient();
            try
            {
                //telemetry.TrackEvent("API-Tags-Get()");
                var tagServer = WebApiApplication.TagServer.Value;
                return GetAPIInfo(tagServer);
            }
            catch (Exception ex)
            {
                Trace.Write(ex);
                telemetry.TrackException(ex); //, properties, measurements);
                return new { Error = ex.ToString().Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries) };
            }
        }

        private object GetAPIInfo(TagServer tagServer)
        {
            var urlRoot = "http://" + Request.RequestUri.Host + ":" + Request.RequestUri.Port + "/api/";
            return new
            {
                SampleUrls = new
                {
                    BasicQueries = new Dictionary<string, string>
                    {
                        { urlRoot + "Tags/c%23", "c# questions (have to escape \"c#\" -> \"c%23\"), defaults to QueryType.ViewCount, pageSize = 50, skip = 0" },
                        { urlRoot + "Tags/c%23?Type=LastActivityDate&PageSize=25", "25 Most Recent c# questions" },
                        { urlRoot + "Tags/c%23?Type=Score&PageSize=1&Skip=71993", "Lowest scoring c# question" },
                        { urlRoot + "Tags/.net?Type=AnswerCount&PageSize=1", "The .NET question with the most answers" }
                    }.ToArray(),
                    AdvancedQueries = new Dictionary<string, string>
                    {
                        { urlRoot + "Query/.net?operator=AND&otherTag=jquery", "AND, i.e. 'C# AND jQuery'" },
                        { urlRoot + "Query/.net?operator=OR&otherTag=jquery", "OR, i.e. 'C# OR jQuery'" },
                        { urlRoot + "Query/.net?operator=OR-NOT&otherTag=jquery", "OR NOT, i.e. 'C# OR NOT jQuery'" },
                        { urlRoot + "Query/.net?operator=OR-NOT&otherTag=jquery&useLinq=true", "OR NOT, i.e. 'C# OR NOT jQuery' BUT using LINQ" },
                        { urlRoot + "Query/.net?operator=NOT&otherTag=jquery", "NOT, i.e. 'C# NOT jQuery'" },
                    }.ToArray(),
                    AdvancedQueryParameters = new Dictionary<string, string>
                    {
                        { "OtherTag", "i.e. 'c# AND jQuery' (&otherTag=jquery)" },
                        { "Type", "Can be " + String.Join(", ", Enum.GetNames(typeof(QueryType))) },
                        { "Operator", "Can be 'AND', 'AND-NOT', 'OR', 'OR-NOT', 'NOT'" },
                        { "PageSize", "1 to 50" },
                        { "Skip", "0 to 'as many as you want!!'" },
                        { "UseLinq", "i.e. '&UseLinq=true' (will be slower than the default mode)" },
                        { "UseLeppieExclusions", "See " + urlRoot + "/Wildcards/LeppieExpandedTags for the full list" },
                        { "DebugMode", "i.e. '&DebugMode=true'" }
                    },
                    //RelatedTagQueries = new Dictionary<string, string>
                    //{
                    //}.ToArray(),
                    Questions = new Dictionary<string, string>
                    {
                        { urlRoot + "Questions/472906", "Get an individual question (by Id)" },
                    }.ToArray(),
                    Wildcards = new Dictionary<string, string>
                    {
                        { urlRoot + "/Wildcards/?wildcards=*c%23*,*java*", "Show all the Tags that matches the given wildcards (comma seperated list, starts-with, end-with or contains only)" },
                        { urlRoot + "/Wildcards/?wildcards=*c%23,c%23*,.net", "Show all the Tags that matches the given wildcards (comma seperated list, starts-with, end-with or contains only)" },
                        { urlRoot + "/Wildcards/?useLeppieWildcards=true",
                            "Show all the Tags that matches the exclusion list from Leppie (see " +
                            urlRoot + "Wildcards/LeppieExpandedTags and " + urlRoot + "Wildcards/LeppieWildcards for the full list)"
                        },
                    }.ToArray(),
                },
                SetupMessages = Logger.Messages,
                Top50Tags = tagServer.AllTags
                                     .Take(50),
                Bottom50Tags = tagServer.AllTags
                                        .OrderBy(t => t.Value)
                                        .Take(50)
            };
        }
    }
}
