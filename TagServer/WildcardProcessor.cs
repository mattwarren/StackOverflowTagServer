using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using HashSet = StackOverflowTagServer.CLR.HashSet<string>;
//using HashSet = System.Collections.Generic.HashSet<string>;

namespace StackOverflowTagServer
{
    static class WildcardProcessor
    {
        /// <summary> 1 </summary>
        internal static readonly int TrieTerminator = 1;

        /// <summary> -1 </summary>
        internal static readonly int TrieReverseTerminator = -1;

        internal static string Reverse(string text)
        {
            if (text == null) 
                return null;

            char[] array = text.ToCharArray();
            Array.Reverse(array);
            return new String(array);
        }

        internal static List<string> ExpandTagsVisualBasic(Dictionary<string, int> allTags, List<string> tagsToExpand)
        {
            // For simplicity use Operators.LikeString from Microsoft.VisualBasic.CompilerServices, see
            // http://stackoverflow.com/questions/6907720/need-to-perform-wildcard-etc-search-on-a-string-using-regex/16737492#16737492
            var expandedTags = new System.Collections.Generic.HashSet<string>();
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

        internal static List<string> ExpandTagsRegex(Dictionary<string, int> allTags, List<string> tagsToExpand)
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
                        if (regex.IsMatch(tag))
                        {
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
                }
                else
                {
                    if (allTags.ContainsKey(tagToExpand))
                        expandedTags.Add(tagToExpand);
                }
            }

            return expandedTags.ToList();
        }

        internal static List<string> ExpandTagsTrie(Dictionary<string, int> allTags, List<string> tagsToExpand, Trie<int> trie)
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
                        DoStartsWithAndEndsWithSearch(trie, tagToExpand, expandedTags);

                        // TODO is there a better way to deal with *php*, i.e. matching in the middle of this string, 
                        // The method below is to do a brute-force search, so kills our perf a bit!!!!
                        var actualTag = tagToExpand.Substring(1, tagToExpand.Length - 2);
                        foreach (var tag in allTags)
                        {
                            if (tag.Key.Contains(actualTag))
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

        private static void DoStartsWithAndEndsWithSearch(Trie<int> trie, string tagToExpand, HashSet expandedTags)
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
#if DEBUG
                    else
                        Console.WriteLine("StartsEndsWith 1 - Rejecting {0}, tagToExpand = {1}, tag = {2}", startWithMatch, tagToExpand, actualTag);
#endif
                }
            }
            else
            {
                var startsWithMatches = trie.GetByPrefix(actualTag);
                foreach (var startWithMatch in startsWithMatches)
                {
                    if (startWithMatch.Value == 1)
                        expandedTags.Add(startWithMatch.Key);
#if DEBUG
                    else
                        Console.WriteLine("StartsEndsWith 2 - Rejecting {0}, tagToExpand = {1}, tag = {2}", startWithMatch, tagToExpand, actualTag);
#endif
                }

                var endsWithMatches = trie.GetByPrefix(Reverse(actualTag));
                foreach (var endWithMatch in endsWithMatches)
                {
                    if (endWithMatch.Value == -1)
                        expandedTags.Add(Reverse(endWithMatch.Key));
#if DEBUG
                    else
                        Console.WriteLine("StartsEndsWith 3 - Rejecting {0}, tagToExpand = {1}, tag = {2}", endWithMatch, tagToExpand, Reverse(actualTag));
#endif
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
#if DEBUG
                else
                    Console.WriteLine("StartsWith - Rejecting {0}, tagToExpand = {1}, tag = {2}", match, tagToExpand, actualTag);
#endif
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
#if DEBUG
                else
                    Console.WriteLine("EndsWith   - Rejecting {0}, tagToExpand = {1}, tag = {2}", match, tagToExpand, Reverse(actualTag));
#endif
            }
        }

        private static bool IsWildCard(string tag)
        {
            return tag.Contains("*");
        }
    }
}
