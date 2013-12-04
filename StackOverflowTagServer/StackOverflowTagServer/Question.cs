using System;
using System.Collections.Generic;
using ProtoBuf;

namespace StackOverflowTagServer
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
    }
}