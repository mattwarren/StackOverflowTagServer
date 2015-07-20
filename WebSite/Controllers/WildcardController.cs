using Server.Infrastructure;
using StackOverflowTagServer;
using System;
using System.Diagnostics;
using System.Linq;
using System.Web;
using System.Web.Http;

namespace Server.Controllers
{
    public class WildcardController : ApiController
    {
        [Route("api/Wildcard/")]
        [HttpGet]
        public object Expand()
        {
            var queryStringPairs = HttpContext.Current.Request.QueryString.ToPairs();
            var wildcards = QueryStringProcessor.GetString(queryStringPairs, "wildcards", "");

            var timer = Stopwatch.StartNew();
            var initialWildcards = wildcards.Split(',').ToList();
            Trace.WriteLine("WildcardsQueryString  = " + wildcards);
            Trace.WriteLine("InitialWildcards = " + String.Join(" - ", initialWildcards));

            var allTags = WebApiApplication.TagServer.Value.AllTags;
            var nGrams = WebApiApplication.NGrams.Value;
            var expandedTags = WildcardProcessor.ExpandTagsNGrams(allTags, initialWildcards, nGrams);
            timer.Stop();

            return new
            {
                ElapsedMilliseconds = timer.Elapsed.TotalMilliseconds.ToString("N2"),
                InitialWildcards = String.Join(" - ", initialWildcards),
                TriGrams = initialWildcards.ToDictionary(w => w, w => WildcardProcessor.CreateSearches(w)),
                ExpandedTagsCount = expandedTags.Count,
                ExpandedTags = expandedTags
            };
        }
    }
}
