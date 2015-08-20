using Server.Infrastructure;
using StackOverflowTagServer;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Web;
using System.Web.Http;

namespace Server.Controllers
{
    public class WildcardController : ApiController
    {
        [Route("api/Wildcards/")]
        [HttpGet]
        public object Expand()
        {
            var queryStringPairs = HttpContext.Current.Request.QueryString.ToPairs();
            var wildcards = QueryStringProcessor.GetString(queryStringPairs, "wildcards", "");
            var useLeppieWildcards = QueryStringProcessor.GetBool(queryStringPairs, "useLeppieWildcards", false);

            var timer = Stopwatch.StartNew();

            var wildcardExpansionTimer = Stopwatch.StartNew();
            var initialWildcards = useLeppieWildcards ? WebApiApplication.LeppieWildcards.Value : wildcards.Split(',').ToList();
            var allTags = WebApiApplication.TagServer.Value.AllTags;
            var nGrams = WebApiApplication.NGrams.Value;
            var expandedTags = WildcardProcessor.ExpandTagsNGrams(allTags, initialWildcards, nGrams);
            wildcardExpansionTimer.Stop();

            //var bitMapTimer = Stopwatch.StartNew();
            //var bitMapIndex = WebApiApplication.TagServer.Value.CreateBitMapIndexForExcludedTags(expandedTags, QueryType.Score);
            //bitMapTimer.Stop();

            timer.Stop();

            return new
            {
                TotalElapsedMilliseconds = timer.Elapsed.TotalMilliseconds.ToString("N2") + " ms",
                WildcardExpansionMilliseconds = wildcardExpansionTimer.Elapsed.TotalMilliseconds.ToString("N2") + " ms",
                //BitMapCreationMilliseconds = bitMapTimer.Elapsed.TotalMilliseconds.ToString("N2") + " ms",
                //QuestionsIncludingExpandedTags = ((ulong)WebApiApplication.TagServer.Value.Questions.Count - bitMapIndex.GetCardinality()).ToString("N0"),
                InitialWildcards = useLeppieWildcards ? "USING Leppie's Wildcards, list to big to print!!!" : String.Join(" - ", initialWildcards),
                ExpandedTagsCount = expandedTags.Count.ToString("N0"),
                ExpandedWildcardCount = initialWildcards.Where(w => w.Contains('*'))
                                                        .ToDictionary(w => w, w => WildcardProcessor.ExpandTagsNGrams(allTags, new List<string>(new[] { w }), nGrams).Count)
                                                        .OrderByDescending(g => g.Value)
                                                        .ThenBy(g => g.Key)
                                                        .ToDictionary(g => g.Key, g => g.Value),
                ExpandedWildcard = initialWildcards.Where(w => w.Contains('*'))
                                                   .ToDictionary(w => w, w => WildcardProcessor.ExpandTagsNGrams(allTags, new List<string>(new[] { w }), nGrams))
                                                   .OrderBy(g => g.Key)
                                                   .ToDictionary(g => g.Key, g => g.Value),
            };
        }

        [Route("api/Wildcards/LeppieWildcards")]
        [HttpGet]
        public object LeppieWildcards()
        {
            var justWildcards = WebApiApplication.LeppieWildcards.Value
                                    .Where(w => w.Contains("*"))
                                    .OrderBy(t => t)
                                    .ToList();
            return new
            {
                Count = WebApiApplication.LeppieWildcards.Value.Count,
                JustWildcardsCount = justWildcards.Count,
                JustWildcards = justWildcards,
                FullList = WebApiApplication.LeppieWildcards.Value.OrderBy(t => t)
            };
        }

        [Route("api/Wildcards/LeppieExpandedTags")]
        [HttpGet]
        public object LeppieExpandedWildcards()
        {
            var allTags = WebApiApplication.TagServer.Value.AllTags;
            var leppieWildcards = WebApiApplication.LeppieWildcards.Value;
            var nGrams = WebApiApplication.NGrams.Value;
            var timer = Stopwatch.StartNew();
            var expandedWildcards = WildcardProcessor.ExpandTagsNGrams(allTags, leppieWildcards, nGrams)
                                                     .OrderBy(t => t)
                                                     .ToList();
            timer.Stop();

            return new
            {
                ElapsedMilliseconds = timer.Elapsed.TotalMilliseconds.ToString("N2"),
                CountBeforeExpansion = leppieWildcards.Count,
                ExpandedCount = expandedWildcards.Count,
                ExpandedWildcards = expandedWildcards
            };
        }
    }
}
