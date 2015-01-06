﻿using Shared;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace StackOverflowTagServer
{
    class Validator
    {
        private List<Question> questions;
        private TagServer.LogAction Log;

        public Validator(List<Question> questions, TagServer.LogAction log)
        {
            this.questions = questions;
            this.Log = log;
        }

        internal void ValidateTags(Dictionary<string, int[]> tagsToCheck, Func<Question, Question, bool> checker)
        {
            foreach (var tag in tagsToCheck)
            {
                Question previous = null;
                var counter = 0;
                foreach (var id in tag.Value)
                {
                    var current = questions[id];
                    if (previous != null)
                    {
                        var result = checker(current, previous);

                        if (!result)
                        {
                            Log("Failed with Id {0}, Tag {1}, checker() returned false", id, tag.Key);
                            var test = tag.Value.Select(t => questions[t]).ToList();
                            //System.Diagnostics.Debugger.Launch();
                            break;
                        }

                        if (current.Tags.Any(t => t == tag.Key) == false)
                        {
                            Log("Failed with Id {0}, Expected Tag {1}, Got Tags {2}", id, tag.Key, string.Join(", ", current.Tags));
                            //System.Diagnostics.Debugger.Launch();
                            break;
                        }
                    }

                    previous = current;
                    counter++;
                }
                if (counter != tag.Value.Count())
                    Log("ERROR - Tag {0}, Checked {1} items, Expected {2}", tag.Key, counter, tag.Value.Count());
            }
        }
    }
}