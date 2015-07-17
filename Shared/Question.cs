using System;
using System.Collections.Generic;
using ProtoBuf;

namespace Shared
{
    [ProtoContract]
    public class Question
    {
        [ProtoMember(1)]
        public long Id { get; set; }
        [ProtoMember(2)]
        public string Title { get; set; }

        [ProtoMember(3)]
        public string RawTags { get; set; }

        private IList<string> tagList = null;
        public IList<string> Tags
        {
            get
            {
                if (tagList == null)
                    tagList =  new List<string>(RawTags.Split(new string[] { " ", "<", ">" }, StringSplitOptions.RemoveEmptyEntries));
                return tagList;
            }
            set
            {
                tagList = value;
            }
        }

        public Uri Url { get { return new Uri("http://stackoverflow.com/questions/" + Id.ToString()); } }

        [ProtoMember(4)]
        public DateTime CreationDate { get; set; }
        [ProtoMember(5)]
        public DateTime LastActivityDate { get; set; }
        [ProtoMember(6)]
        public int? Score { get; set; }
        [ProtoMember(7)]
        public int? ViewCount { get; set; }
        [ProtoMember(8)]
        public int? AnswerCount { get; set; }
        [ProtoMember(9)]
        public int? AcceptedAnswerId { get; set; }

        public override string ToString()
        {
            return base.ToString();

            // Print the full Question for each result (this is harder to read!!
            //Console.WriteLine("\t{0}", string.Join("\n\t", result.Select(r => new
            //                                                    {
            //                                                        r.Id,
            //                                                        Tags = string.Join(", ", r.Tags),
            //                                                        r.LastActivityDate,
            //                                                        r.CreationDate,
            //                                                        r.Score,
            //                                                        r.ViewCount,
            //                                                        r.AnswerCount
            //                                                    })));
        }
    }
}