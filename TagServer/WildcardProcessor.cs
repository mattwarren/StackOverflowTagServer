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
//using HashSet = System.Collections.Generic.HashSet<string>;
using NGrams = System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<int>>;
using TagLookup = System.Collections.Generic.Dictionary<string, int>;

namespace StackOverflowTagServer
{
// ReSharper disable LocalizableElement
    static class WildcardProcessor
    {
        /// <summary> 1 </summary>
        private static readonly int TrieTerminator = 1;

        /// <summary> -1 </summary>
        private static readonly int TrieReverseTerminator = -1;

        private static readonly char WordAnchor = '^';

        internal static Trie<int> CreateTrie(TagLookup allTags)
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

            Console.WriteLine("\nTook {0} ({1:N2} ms) to SETUP the Trie (ONE-OFF cost)", trieSetupTimer.Elapsed, trieSetupTimer.Elapsed.TotalMilliseconds);

            return trie;
        }

        internal static NGrams CreateNGrams(IEnumerable<string> allTags, int N)
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
            foreach (var item in allTags.Select((tag, posn) => new { Tag = tag, Posn = posn }))
            {
                var nGrams = CreateNGramsForIndexing(item.Tag, N).ToList();
                //var expected = Math.Max(1, item.Tag.Length - N + 1);
                var expected = Math.Max(1, item.Tag.Length - N + 1 + 2);
                if (expected != nGrams.Count)
                {
                    Console.WriteLine("n-grams (n={0}) for \"{1}\" (Got: {2}, Expected: {3}): {4} ",
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
            Console.WriteLine("\nTook {0} to create {1:N0} n-grams (ONE-OFF cost)\n", nGramsTimer.Elapsed, allNGrams.Count);



            return allNGrams;
        }

        internal static List<string> ExpandTagsVisualBasic(TagLookup allTags, List<string> tagsToExpand)
        {
            // For simplicity use Operators.LikeString from Microsoft.VisualBasic.CompilerServices, see
            // http://stackoverflow.com/questions/6907720/need-to-perform-wildcard-etc-search-on-a-string-using-regex/16737492#16737492
            var expandedTags = new HashSet<string>();
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
                else
                {
                    if (allTags.ContainsKey(tagToExpand))
                        expandedTags.Add(tagToExpand);
                }
            }

            return expandedTags.ToList();
        }

        internal static List<string> ExpandTagsRegex(TagLookup allTags, List<string> tagsToExpand)
        {
            // See http://www.c-sharpcorner.com/uploadfile/b81385/efficient-string-matching-algorithm-with-use-of-wildcard-characters/
            // or http://www.henrikbrinch.dk/Blog/2013/03/07/Wildcard-Matching-In-C-Using-Regular-Expressions
            var expandedTags = new HashSet();
            //var wierdMatches = new[] { "access-vba", "word-vba", "microsoft-project-vba", "visio-vba", "xmlhttp-vba" };
            var wierdMatches = new[] { "cakephp", "node-postgres", "libsvm", "rsolr" };
            foreach (var tagToExpand in tagsToExpand)
            {
                if (IsWildCard(tagToExpand))
                {
                    var regexPattern = "^" + Regex.Escape(tagToExpand).Replace("\\*", ".*") + "$";
                    Regex regex = new Regex(regexPattern, RegexOptions.Compiled);
                    foreach (var tag in allTags.Keys)
                    {
                        if (!regex.IsMatch(tag)) 
                            continue;

                        expandedTags.Add(tag);

                        //Added "access-vba", tagToExpand = "*-vba", regex pattern = ^.*-vba$
                        //Added "word-vba", tagToExpand = "*-vba", regex pattern = ^.*-vba$
                        //Added "microsoft-project-vba", tagToExpand = "*-vba", regex pattern = ^.*-vba$
                        //Added "visio-vba", tagToExpand = "*-vba", regex pattern = ^.*-vba$
                        //Added "xmlhttp-vba", tagToExpand = "*-vba", regex pattern = ^.*-vba$
                        //if (wierdMatches.Contains(tag))
                        //{
                        //    Console.WriteLine("Added \"{0}\", tagToExpand = \"{1}\", regex pattern = {2}", tag, tagToExpand, regexPattern);
                        //}
                    }
                }
                else
                {
                    if (allTags.ContainsKey(tagToExpand))
                        expandedTags.Add(tagToExpand);
                }
            }

            return expandedTags.ToList();
        }

        internal static List<string> ExpandTagsTrie(TagLookup allTags, List<string> tagsToExpand, Trie<int> trie, bool useNewMode = true)
        {
            // It *seems* like SO only allows prefix, suffix or both, i.e. "java*", "*-vba", "*java*"
            // But not anything else like "ja*a", or "j?va?", etc
            var expandedTags = new HashSet();
            foreach (var tagToExpand in tagsToExpand)
            {
                if (IsWildCard(tagToExpand) == false)
                {
                    int value;
                    if (trie.TryGetValue(tagToExpand, out value))
                        expandedTags.Add(tagToExpand);
                }
                else
                {
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

                        // TODO is there a better way to deal with *php*, i.e. matching in the middle of this string, 
                        // The method below is to do a brute-force search, so kills our perf a bit!!!!
                        // If we don't do this, we miss items that are in the middle i.e. *php* won't match "cakephp-1.0"
                        foreach (var tag in allTags)
                        {
                            if (tag.Key.Contains(actualTag) && expandedTags.Contains(tag.Key) == false)
                                expandedTags.Add(tag.Key);
                        }
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
            }

            return expandedTags.ToList();
        }

        internal static HashSet<string> ExpandTagsNGrams(TagLookup allTags, List<string> tagsToExpand, NGrams nGrams)
        {
            // Query: /Google.*Search/, we can build a query of ANDs and ORs that gives the trigrams that must be present in any text matching the regular expression. 
            // In this case, the query is
            //      Goo AND oog AND ogl AND gle AND Sea AND ear AND arc AND rch
            // '*php* -> php
            // '*.net* -> .ne AND net
            // '*.net' -> .ne AND net // how do we distinguish this from '*.net*'
            // *hibernate* -> hib AND ibe AND ber AND ern AND rna AND nat AND ate

            var expandedTags = new HashSet<string>();
            var tagsExpandedCounter = 0;
            foreach (var tagPattern in tagsToExpand)
            {
                if (tagPattern.Contains("*") == false)
                {
                    //not a wildcard, leave it as is
                    if (allTags.ContainsKey(tagPattern))
                        expandedTags.Add(tagPattern);
                    continue; 
                }

                var searches = Enumerable.Empty<string>();
                var actualTag = String.Empty;
                var createSearchTimer = Stopwatch.StartNew();
                if (tagPattern.StartsWith("*") && tagPattern.EndsWith("*"))
                {
                    // "anywhere" wildcard, i.e. "*foo*"
                    actualTag = tagPattern.Substring(1, tagPattern.Length - 2);
                    searches = CreateNGramsForSearch(actualTag, N: 3).ToList();
                }
                else if (tagPattern.EndsWith("*"))
                {
                    // "starts-with" or prefix search, i.e "foo*"
                    actualTag = tagPattern.Substring(0, tagPattern.Length - 1);
                    searches = new[] { WordAnchor + actualTag.Substring(0, 2) }
                        .Concat(CreateNGramsForSearch(actualTag, N: 3))
                        .ToList();
                }
                else if (tagPattern.StartsWith("*"))
                {
                    // "end-with" or suffix search, i.e "*foo"
                    actualTag = tagPattern.Substring(1, tagPattern.Length - 1);
                    searches = CreateNGramsForSearch(actualTag, N: 3)
                                .Concat(new[] { actualTag.Substring(tagPattern.Length - 3, 2) + WordAnchor })
                                .ToList();
                }
                
                createSearchTimer.Stop();
                //if (searches.Any())
                //    Console.WriteLine("Took {0} ({1,5:N0} ms), Tag: \"{2}\" ({3}) -> {4}", createSearchTimer.Elapsed,
                //                  createSearchTimer.ElapsedMilliseconds, tagPattern, actualTag, String.Join(", ", searches));

                var tagAdded = CollectPossibleNGramMatches(allTags, nGrams, searches, tagPattern, expandedTags);

                if (tagAdded)
                    tagsExpandedCounter++;
            }
            return expandedTags;
        }

        private static bool CollectPossibleNGramMatches(TagLookup allTags, NGrams nGrams, IEnumerable<string> searches, string tagPattern, HashSet<string> expandedTags)
        {
            var expandTagsTimer = Stopwatch.StartNew();
            HashSet<int> expandedTagIds = null;
            foreach (var search in searches)
            {
                if (nGrams.ContainsKey(search))
                {
                    var tagLocations = nGrams[search];
                    if (expandedTagIds == null)
                        expandedTagIds = new HashSet<int>(tagLocations);
                    else
                        expandedTagIds.IntersectWith(tagLocations);
                }
            }

            //var expandedTags = (expandedTagIds ?? Enumerable.Empty<int>()).Select(id => allTagsList[id]).ToList();
            //allExpandedTags.AddRange(expandedTags);

            var regexPattern = "^" + Regex.Escape(tagPattern).Replace("\\*", ".*") + "$";
            var regex = new Regex(regexPattern, RegexOptions.Compiled);
            bool tagAdded = false;
            // TODO is there a better way of doing this, we have to create a tempoary list, just for indexing the dictionary!!!
            var allTagsList = allTags.Keys.ToList();
            foreach (var tagMatch in expandedTagIds.Select(expandedTagId => allTagsList[expandedTagId]))
            {
                if (regex.IsMatch(tagMatch))
                {
                    expandedTags.Add(tagMatch);
                    tagAdded = true;                    
                }
                else
                {
                    //Console.WriteLine("False Positive, Tag: {0}, TagPattern: {1}, Searches: {2}",
                    //                  tagMatch, tagPattern, String.Join(", ", searches));
                }
            }
            expandTagsTimer.Stop();

            //Console.WriteLine("Took {0} ({1,5:N0} ms), to expand to {2} Tags:", 
            //                  expandTagsTimer.Elapsed, expandTagsTimer.ElapsedMilliseconds, expandedTags.Count);
            //Console.WriteLine(String.Join(", ", expandedTags));
            //Console.WriteLine();
            return tagAdded;
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
//#if DEBUG
//                    else
//                        Console.WriteLine("StartsEndsWith 1 - Rejecting {0}, tagToExpand = {1}, tag = {2}", startWithMatch, tagToExpand, actualTag);
//#endif
                }
            }
            else
            {
                var startsWithMatches = trie.GetByPrefix(actualTag);
                foreach (var startWithMatch in startsWithMatches)
                {
                    if (startWithMatch.Value == 1)
                        expandedTags.Add(startWithMatch.Key);
//#if DEBUG
//                    else
//                        Console.WriteLine("StartsEndsWith 2 - Rejecting {0}, tagToExpand = {1}, tag = {2}", startWithMatch, tagToExpand, actualTag);
//#endif
                }

                var endsWithMatches = trie.GetByPrefix(Reverse(actualTag));
                foreach (var endWithMatch in endsWithMatches)
                {
                    if (endWithMatch.Value == -1)
                        expandedTags.Add(Reverse(endWithMatch.Key));
//#if DEBUG
//                    else
//                        Console.WriteLine("StartsEndsWith 3 - Rejecting {0}, tagToExpand = {1}, tag = {2}", endWithMatch, tagToExpand, Reverse(actualTag));
//#endif
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
//#if DEBUG
//                else
//                    Console.WriteLine("StartsWith - Rejecting {0}, tagToExpand = {1}, tag = {2}", match, tagToExpand, actualTag);
//#endif
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
//#if DEBUG
//                else
//                    Console.WriteLine("EndsWith   - Rejecting {0}, tagToExpand = {1}, tag = {2}", match, tagToExpand, Reverse(actualTag));
//#endif
            }
        }

        private static string Reverse(string text)
        {
            if (text == null)
                return null;

            char[] array = text.ToCharArray();
            Array.Reverse(array);
            return new String(array);
        }

        private static bool IsWildCard(string tag)
        {
            return tag.Contains("*");
        }
    }
// ReSharper restore LocalizableElement
}
