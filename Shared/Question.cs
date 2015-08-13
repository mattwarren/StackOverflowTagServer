using System;
using System.Collections.Generic;
using ProtoBuf;
using System.Linq;

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
                {
                    tagList = new List<string>(
                                    RawTags.Split(new string[] { " ", "<", ">" }, StringSplitOptions.RemoveEmptyEntries)
                                           .Select(s => String.Intern(s)));
                }

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

            // Print the full Question for each result (this is harder to read!!)
            //Logger.Log("\t{0}", string.Join("\n\t", result.Select(r => new
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

        public override bool Equals(object obj)
        {
            // If parameter is null return false.
            if (obj == null)
            {
                return false;
            }

            // If parameter cannot be cast to Question return false.
            Question other = obj as Question;
            if (other == null)
            {
                return false;
            }

            // Return true if the fields match:
            return this.Equals(other);
        }

        public bool Equals(Question other)
        {
            // If parameter is null return false:
            if (other == null)
            {
                return false;
            }

            return Id == other.Id;
        }


        public override int GetHashCode()
        {
            return Id.GetHashCode();
        }
    }
}