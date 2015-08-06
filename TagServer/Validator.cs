using Shared;
using System;
using System.Collections.Generic;
using System.Linq;
using Ewah;
using System.Diagnostics;

namespace StackOverflowTagServer
{
    class Validator
    {
        private readonly List<Question> questions;

        public Validator(List<Question> questions)
        {
            this.questions = questions;
        }

        internal void ValidateItems(Dictionary<string, IEnumerable<int>> itemsToCheck,
                                    Func<Question, Question, bool> checker,
                                    string info,
                                    int[] questionLookup = null)
        {
            var timer = Stopwatch.StartNew();
            var globalCounter = 0;
            foreach (var item in itemsToCheck)
            {
                Question previous = null;
                var counter = 0;
                var tag = item.Key;
                foreach (var id in item.Value)
                {
                    Question current = questionLookup == null ? questions[id] : questions[questionLookup[id]];
                    if (previous != null)
                    {
                        var result = checker(current, previous);

                        if (!result)
                        {
                            Logger.LogStartupMessage("Failed with Id {0}, Tag {1}, checker() returned false", id, tag);
                            break;
                        }

                        if (tag != TagServer.ALL_TAGS_KEY && current.Tags.Any(t => t == tag) == false)
                        {
                            Logger.LogStartupMessage("Failed with Id {0}, Expected Tag {1}, Got Tags {2}", id, tag, string.Join(", ", current.Tags));
                            break;
                        }
                    }

                    previous = current;
                    counter++;
                }

                globalCounter += counter;
                if (counter != item.Value.Count())
                    Logger.LogStartupMessage("ERROR - Tag {0}, Checked {1} items, Expected to Check {2} items", tag, counter, item.Value.Count());
            }
            timer.Stop();

            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to SUCCESSFULLY validate {2:N0} items -> {3}",
                                     timer.Elapsed, timer.ElapsedMilliseconds, globalCounter, info);
        }
    }
}
