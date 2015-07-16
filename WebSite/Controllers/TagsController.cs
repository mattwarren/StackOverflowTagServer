using Server.Infrastructure;
using StackOverflowTagServer.DataStructures;
using System.Diagnostics;
using System.Linq;
using System.Web;
using System.Web.Http;

namespace Server.Controllers
{
    [ActionWebApiFilter]
    public class TagsController : ApiController
    {
        [Route("api/Tags/{tag}")]
        [HttpGet]
        public object Get(string tag)
        {
            // Couldn't get Web API to play nice with "c%23" ("c#") and query parameters?!?
            var queryStringPairs = HttpContext.Current.Request.QueryString.ToPairs();
            var type = QueryStringProcessor.GetEnum(queryStringPairs, "type", QueryType.ViewCount);
            var pageSize = QueryStringProcessor.GetInt(queryStringPairs, "pageSize", 50);
            var skip = QueryStringProcessor.GetInt(queryStringPairs, "skip", 0);

            // TODO add a "Descending" option

            var timer = Stopwatch.StartNew();
            var results = WebApiApplication.TagServer.Value.Query(type, tag, pageSize, skip);
            timer.Stop();

            return new
            {
                Statistics = new {
                    Elapsed = timer.Elapsed,
                    ElapsedMilliseconds = timer.Elapsed.TotalMilliseconds.ToString("N2"),
                    Count = results.Count,
                    TotalQuestionsForTag = WebApiApplication.TagServer.Value.TotalCount(type, tag)
                },
                DEBUGGING = new
                {
                    Tag = tag,
                    QueryType = type.ToString(),
                    PageSize = pageSize,
                    Skip = skip,
                    HttpContext.Current.Request.Path,
                    HttpContext.Current.Request.RawUrl,
                    QueryString = HttpContext.Current.Request.QueryString
                                            .ToPairs()
                                            .ToDictionary(p => p.Key, p => p.Value)
                },
                Results = results,
            };
        }
    }
}
