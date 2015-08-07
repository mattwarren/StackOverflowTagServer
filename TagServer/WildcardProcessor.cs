using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

using HashSet = StackOverflowTagServer.CLR.HashSet<string>;
using NGrams = System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<int>>;
using TagLookup = System.Collections.Generic.Dictionary<string, int>;

// ReSharper disable LocalizableElement
namespace StackOverflowTagServer
{
    public static class WildcardProcessor
    {
        /// <summary> 1 </summary>
        private static readonly int TrieTerminator = 1;

        /// <summary> -1 </summary>
        private static readonly int TrieReverseTerminator = -1;

        /// <summary> The Word Anchor is a '^' character </summary>
        private static readonly char WordAnchor = '^';

        /// <summary> The N-Grams value, i.e. 2, 3, etc </summary>
        public static readonly int N = 2; // We use 2 as 3 doesn't allow '*c#*' wildcards (but '*c#' and 'c#*' work),

        public static Trie<int> CreateTrie(TagLookup allTags)
        {
            // From http://algs4.cs.princeton.edu/52trie/
            // 15. Substring matches.
            //     Given a list of (short) strings, your goal is to support queries where the user looks up a string s
            //     and your job is to report back all strings in the list that contain s.
            //     Hint: if you only want prefix matches (where the strings have to start with s), use a TST as described in the text.
            //     To support substring matches, insert the suffixes of each word (e.g., string, tring, ring, ing, ng, g) into the TST.
            var trieSetupTimer = Stopwatch.StartNew();
            var trie = new Trie<int>();
            trie.AddRange(allTags.Select(t => new TrieEntry<int>(t.Key, TrieTerminator)));
            trie.AddRangeAllowDuplicates(allTags.Select(t => new TrieEntry<int>(Reverse(t.Key), TrieReverseTerminator)));
            trieSetupTimer.Stop();

            Logger.LogStartupMessage("\nTook {0} ({1,6:N2} ms) to SETUP the Trie (ONE-OFF cost)", trieSetupTimer.Elapsed, trieSetupTimer.Elapsed.TotalMilliseconds);

            return trie;
        }

        public static NGrams CreateNGrams(TagLookup allTags)
        {
            // From https://swtch.com/~rsc/regexp/regexp4.html,
            // Continuing the example from the last section, the document set:
            // (1) Google Code Search
            // (2) Google Code Project Hosting
            // (3) Google Web Search

            // has this trigram index:
            // _Co: {1, 2}     Sea: {1, 3}     e_W: {3}        ogl: {1, 2, 3}
            // _Ho: {2}        Web: {3}        ear: {1, 3}     oje: {2}
            // _Pr: {2}        arc: {1, 3}     eb_: {3}        oog: {1, 2, 3}
            // _Se: {1, 3}     b_S: {3}        ect: {2}        ost: {2}
            // _We: {3}        ct_: {2}        gle: {1, 2, 3}  rch: {1, 3}
            // Cod: {1, 2}     de_: {1, 2}     ing: {2}        roj: {2}
            // Goo: {1, 2, 3}  e_C: {1, 2}     jec: {2}        sti: {2}
            // Hos: {2}        e_P: {2}        le_: {1, 2, 3}  t_H: {2}
            // Pro: {2}        e_S: {1}        ode: {1, 1}     tin: {2}
            // (The _ character serves here as a visible representation of a space.)

            var nGramsTimer = Stopwatch.StartNew();
            var allNGrams = new NGrams();
            foreach (var item in allTags.Select((item, posn) => new { Tag = item.Key, Posn = posn }))
            {
                // TODO only use .ToList() here because we call .Count, if that was removed, we could lose the ToList() call
                var nGrams = CreateNGramsForIndexing(item.Tag, N).ToList();
                var expected = Math.Max(1, item.Tag.Length - N + 1 + 2);
                if (expected != nGrams.Count)
                {
                    Logger.LogStartupMessage("n-grams (n={0}) for \"{1}\" (Got: {2}, Expected: {3}): {4} ",
                                             N, item.Tag, nGrams.Count, expected, String.Join(", ", nGrams));
                }

                foreach (var nGram in nGrams)
                {
                    if (allNGrams.ContainsKey(nGram) == false)
                    {
                        var locations = new List<int>(10) { item.Posn };
                        allNGrams.Add(nGram, locations);
                    }
                    else
                    {
                        allNGrams[nGram].Add(item.Posn);
                    }
                }
            }
            nGramsTimer.Stop();
            Logger.LogStartupMessage("\nTook {0} ({1,6:N2} ms) to create {2:N0} N-Grams (with {3:N0} positions), using N={4} (this is a ONE-OFF cost)\n",
                                     nGramsTimer.Elapsed, nGramsTimer.Elapsed.TotalMilliseconds, allNGrams.Count, allNGrams.Sum(n => n.Value.Count), N);

            return allNGrams;
        }

        internal static HashSet ExpandTagsContainsStartsWithEndsWith(TagLookup allTags, List<string> tagsToExpand)
        {
            var expandedTags = new HashSet();
            foreach (var tagToExpand in tagsToExpand)
            {
                if (IsWildCard(tagToExpand))
                {
                    var rawTagPattern = tagToExpand.Replace("*", "");
                    foreach (var tag in allTags.Keys)
                    {
                        if (IsActualMatch(tag, tagToExpand, rawTagPattern))
                            expandedTags.Add(tag);
                    }
                }
                else if (allTags.ContainsKey(tagToExpand))
                {
                    expandedTags.Add(tagToExpand);
                }
            }

            return expandedTags;
        }

        internal static HashSet ExpandTagsVisualBasic(TagLookup allTags, List<string> tagsToExpand)
        {
            // For simplicity use Operators.LikeString from Microsoft.VisualBasic.CompilerServices, see
            // http://stackoverflow.com/questions/6907720/need-to-perform-wildcard-etc-search-on-a-string-using-regex/16737492#16737492
            var expandedTags = new HashSet();
            foreach (var tagToExpand in tagsToExpand)
            {
                if (IsWildCard(tagToExpand))
                {
                    foreach (var tag in allTags.Keys)
                    {
                        if (Operators.LikeString(tag, tagToExpand, CompareMethod.Text))
                        {
                            //If you use CompareMethod.Text it will compare case-insensitive.
                            //For case-sensitive comparison, you can use CompareMethod.Binary.
                            expandedTags.Add(tag);
                        }
                    }
                }
                else if (allTags.ContainsKey(tagToExpand))
                {
                    expandedTags.Add(tagToExpand);
                }
            }

            return expandedTags;
        }

        internal static HashSet ExpandTagsRegex(TagLookup allTags, List<string> tagsToExpand)
        {
            var expandedTags = new HashSet();
            //var wierdMatches = new[] { "access-vba", "word-vba", "microsoft-project-vba", "visio-vba", "xmlhttp-vba" };
            //var wierdMatches = new[] { "cakephp", "node-postgres", "libsvm", "rsolr" };
            foreach (var tagToExpand in tagsToExpand)
            {
                if (IsWildCard(tagToExpand))
                {
                    // See http://www.c-sharpcorner.com/uploadfile/b81385/efficient-string-matching-algorithm-with-use-of-wildcard-characters/
                    // or http://www.henrikbrinch.dk/Blog/2013/03/07/Wildcard-Matching-In-C-Using-Regular-Expressions
                    var regexPattern = "^" + Regex.Escape(tagToExpand).Replace("\\*", ".*") + "$";
                    var regex = new Regex(regexPattern, RegexOptions.Compiled);
                    foreach (var tag in allTags.Keys)
                    {
                        if (!regex.IsMatch(tag))
                            continue;
                        expandedTags.Add(tag);
                    }
                }
                else if (allTags.ContainsKey(tagToExpand))
                {
                   expandedTags.Add(tagToExpand);
                }
            }

            return expandedTags;
        }

        internal static HashSet ExpandTagsTrie(TagLookup allTags, List<string> tagsToExpand, Trie<int> trie, bool useNewMode = true)
        {
            // It *seems* like SO only allows prefix, suffix or both, i.e. "java*", "*-vba", "*java*"
            // But not anything else like "ja*a", or "j?va?", etc
            var expandedTags = new HashSet();
            var bruteForceTimer = new Stopwatch();
            foreach (var tagToExpand in tagsToExpand)
            {
                if (IsWildCard(tagToExpand) == false)
                {
                    if (allTags.ContainsKey(tagToExpand))
                        expandedTags.Add(tagToExpand);

                    continue;
                }

                var firstChar = tagToExpand[0];
                var lastChar = tagToExpand[tagToExpand.Length - 1];
                if (firstChar == '*' && lastChar == '*')
                {
                    var actualTag = tagToExpand.Substring(1, tagToExpand.Length - 2);
                    if (useNewMode)
                    {
                        DoStartsWithSearch(trie, actualTag + "*", expandedTags);
                        DoEndsWithSearch(trie, "*" + actualTag, expandedTags);
                    }
                    else
                    {
                        DoStartsWithOrEndsWithSearch(trie, tagToExpand, expandedTags);
                    }

                    // With a trie, brute-search is the only way to deal with *php*, i.e. matching in the middle of this string,
                    // If we don't do this, we miss items that are in the middle i.e. *php* won't match "cakephp-1.0"
                    bruteForceTimer.Start();
                    foreach (var tag in allTags)
                    {
                        if (tag.Key.Contains(actualTag) && expandedTags.Contains(tag.Key) == false)
                            expandedTags.Add(tag.Key);
                    }
                    bruteForceTimer.Stop();
                }
                else if (lastChar == '*')
                {
                    DoStartsWithSearch(trie, tagToExpand, expandedTags);
                }
                else if (firstChar == '*')
                {
                    DoEndsWithSearch(trie, tagToExpand, expandedTags);
                }
            }

            Logger.Log("Took {0} ({1,6:N2} ms) for Trie expansion JUST to do brute force searches", bruteForceTimer.Elapsed, bruteForceTimer.ElapsedMilliseconds);

            return expandedTags;
        }

        public static HashSet ExpandTagsNGrams(TagLookup allTags, List<string> tagsToExpand, NGrams nGrams)
        {
            // Query: /Google.*Search/, we can build a query of ANDs and ORs that gives the trigrams that must be present in any text matching the regular expression.
            // In this case, the query is
            //      Goo AND oog AND ogl AND gle AND Sea AND ear AND arc AND rch
            // '*php* -> php
            // '*.net* -> .ne AND net
            // '*.net' -> .ne AND net // how do we distinguish this from '*.net*'
            // *hibernate* -> hib AND ibe AND ber AND ern AND rna AND nat AND ate

            var expandedTags = new HashSet();
            // TODO is there a better way of doing this, as we are creating a tempoary list, just for indexing the dictionary!!!
            var allTagsList = allTags.Keys.ToList();
            foreach (var tagPattern in tagsToExpand)
            {
                if (IsWildCard(tagPattern) == false)
                {
                    //not a wildcard, leave it as is
                    if (allTags.ContainsKey(tagPattern))
                        expandedTags.Add(tagPattern);
                    continue;
                }

                var searches = CreateSearches(tagPattern);
                var tagAdded = CollectPossibleNGramMatches(allTagsList, nGrams, searches, tagPattern, expandedTags);
            }
            return expandedTags;
        }

        public static List<string> CreateSearches(string tagPattern)
        {
            var searches = new List<string>();
            var actualTag = String.Empty;
            var firstChar = tagPattern[0];
            var lastChar = tagPattern[tagPattern.Length - 1];
            if (firstChar == '*' && lastChar == '*')
            {
                // "anywhere" wildcard, i.e. "*foo*"
                actualTag = tagPattern.Substring(1, tagPattern.Length - 2);
                searches.AddRange(CreateNGramsForSearch(actualTag, N));
            }
            else if (lastChar == '*')
            {
                // "starts-with" or prefix search, i.e "foo*"
                actualTag = tagPattern.Substring(0, tagPattern.Length - 1);
                searches.Add(WordAnchor + actualTag.Substring(0, 2));
                searches.AddRange(CreateNGramsForSearch(actualTag, N));
            }
            else if (firstChar == '*')
            {
                // "end-with" or suffix search, i.e "*foo"
                actualTag = tagPattern.Substring(1, tagPattern.Length - 1);
                searches.AddRange(CreateNGramsForSearch(actualTag, N));
                searches.Add(actualTag.Substring(tagPattern.Length - 3, 2) + WordAnchor);
            }

            return searches;
        }

        private static bool CollectPossibleNGramMatches(List<string> allTagsList, NGrams nGrams, IEnumerable<string> searches, string tagPattern, HashSet expandedTags)
        {
            HashSet<int> expandedTagIds = null;
            foreach (var search in searches)
            {
                // Sanity check, in case there is a tag in the exclusion list that is no longer a real tag
                if (nGrams.ContainsKey(search))
                {
                    var tagLocations = nGrams[search];
                    if (expandedTagIds == null)
                        expandedTagIds = new HashSet<int>(tagLocations);
                    else
                        expandedTagIds.IntersectWith(tagLocations);
                }
            }

            var tagsAdded = 0;
            var rawTagPattern = tagPattern.Replace("*", "");
            if (expandedTagIds != null)
            {
                foreach (var tagMatch in expandedTagIds.Select(expandedTagId => allTagsList[expandedTagId]))
                {
                    if (IsActualMatch(tagMatch, tagPattern, rawTagPattern))
                    {
                        expandedTags.Add(tagMatch);
                        tagsAdded++;
                    }
                    //else
                    //{
                    //    Logger.Log("False Positive, Tag: {0}, TagPattern: {1}, Searches: {2}",
                    //               tagMatch, tagPattern, String.Join(", ", searches));
                    //}
                }
            }

            return tagsAdded > 0;
        }

        // Heavily-modified version of the code from http://jakemdrew.com/blog/ngram.htm
        private static IEnumerable<string> CreateNGramsForIndexing(string text, int N)
        {
            if (N == 0)
                throw new Exception("n-gram size must be > 0");

            var nGram = new StringBuilder();
            nGram.Append(WordAnchor);
            int currentWordLength = 1;
            for (int i = 0; i < text.Length; i++)
            {
                nGram.Append(text[i]);
                currentWordLength++;

                if (currentWordLength >= N)
                {
                    yield return nGram.ToString();
                    nGram.Remove(0, 1);
                    currentWordLength--;
                }
            }

            nGram.Append(WordAnchor);
            yield return nGram.ToString();
        }

        private static IEnumerable<string> CreateNGramsForSearch(string text, int N)
        {
            if (N == 0)
                throw new Exception("n-gram size must be > 0");

            var nGram = new StringBuilder();
            int currentWordLength = 0;
            for (int i = 0; i < text.Length - 1; i++)
            {
                nGram.Append(text[i]);
                currentWordLength++;

                if (currentWordLength >= N)
                {
                    yield return nGram.ToString();
                    nGram.Remove(0, 1);
                    currentWordLength--;
                }
            }

            nGram.Append(text.Last());
            yield return nGram.ToString();
        }

        private static void DoStartsWithOrEndsWithSearch(Trie<int> trie, string tagToExpand, HashSet expandedTags)
        {
            // STARTS-with OR ENDS-with, i.e. *facebook*
            var actualTag = tagToExpand.Substring(1, tagToExpand.Length - 2);
            if (actualTag == Reverse(actualTag))
            {
                // Palindromes like *php* are a special case, we only need to do a StartWith, for Value = 1 OR -1
                var startsWithMatches = trie.GetByPrefix(actualTag);
                foreach (var startWithMatch in startsWithMatches)
                {
                    if (startWithMatch.Value == TrieTerminator)
                        expandedTags.Add(startWithMatch.Key);
                    else if (startWithMatch.Value == TrieReverseTerminator)
                        expandedTags.Add(Reverse(startWithMatch.Key));
                }
            }
            else
            {
                var startsWithMatches = trie.GetByPrefix(actualTag);
                foreach (var startWithMatch in startsWithMatches)
                {
                    if (startWithMatch.Value == 1)
                        expandedTags.Add(startWithMatch.Key);
                }

                var endsWithMatches = trie.GetByPrefix(Reverse(actualTag));
                foreach (var endWithMatch in endsWithMatches)
                {
                    if (endWithMatch.Value == -1)
                        expandedTags.Add(Reverse(endWithMatch.Key));
                }
            }
        }

        private static void DoStartsWithSearch(Trie<int> trie, string tagToExpand, HashSet expandedTags)
        {
            // STARTS-with, i.e java*
            var actualTag = tagToExpand.Substring(0, tagToExpand.Length - 1);
            var matches = trie.GetByPrefix(actualTag);
            foreach (var match in matches)
            {
                if (match.Value == 1)
                    expandedTags.Add(match.Key);
            }
        }

        private static void DoEndsWithSearch(Trie<int> trie, string tagToExpand, HashSet expandedTags)
        {
            // ENDS-with, i.e. *script
            var actualTag = tagToExpand.Substring(1, tagToExpand.Length - 1);
            var matches = trie.GetByPrefix(Reverse(actualTag));
            foreach (var match in matches)
            {
                if (match.Value == -1)
                    expandedTags.Add(Reverse(match.Key));
            }
        }

        private static string Reverse(string text)
        {
            if (text == null)
                return null;

            // Yes I know, this fails badly with Unicode!!!
            char[] array = text.ToCharArray();
            Array.Reverse(array);
            return new String(array);
        }

        private static bool IsActualMatch(string tagMatch, string tagPattern, string rawTagPattern)
        {
            // NOTE this ONLY works if '*' can only be at the front/end/both, i.e. '*foo', 'foo*' or '*foo*'
            // if wildcards '*' are allowed elsewhere (i.e. 'fo*o'), we have to make it more complex!
            var firstChar = tagPattern[0];
            var lastChar = tagPattern[tagPattern.Length - 1];
            if (firstChar == '*' && lastChar == '*')
            {
                // "anywhere" wildcard, i.e. "*foo*"
                if (tagMatch.Contains(rawTagPattern))
                    return true;
            }
            else if (lastChar == '*')
            {
                // "starts-with" or prefix search, i.e "foo*"
                if (tagMatch.StartsWith(rawTagPattern))
                    return true;
            }
            else if (firstChar == '*')
            {
                // "end-with" or suffix search, i.e "*foo"
                if (tagMatch.EndsWith(rawTagPattern))
                    return true;
            }

            return false;
        }

        private static bool IsWildCard(string tag)
        {
            return tag.Contains("*");
        }
    }
}
// ReSharper restore LocalizableElement