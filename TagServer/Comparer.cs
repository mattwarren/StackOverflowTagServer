using Shared;
using System;
using System.Collections.Generic;

namespace StackOverflowTagServer
{
    public class Comparer
    {
        private readonly List<Question> questions;

        public Comparer(List<Question> questions)
        {
            this.questions = questions;
        }

        public int CompareId(int x, int y)
        {
            // Compare Ids in the usual order, i.e. ASCENDING
            return questions[x].Id.CompareTo(questions[y].Id);
        }

        public int CreationDate(int x, int y)
        {
            if (questions[y].CreationDate == questions[x].CreationDate)
                return CompareId(x, y);
            // Compare CreationDates DESCENDING, i.e. most recent is first
            return questions[y].CreationDate.CompareTo(questions[x].CreationDate);
        }

        public int LastActivityDate(int x, int y)
        {
            if (questions[y].LastActivityDate == questions[x].LastActivityDate)
                return CompareId(x, y);
            // Compare LastActivityDate DESCENDING, i.e. most recent is first
            return questions[y].LastActivityDate.CompareTo(questions[x].LastActivityDate);
        }

        public int Score(int x, int y)
        {
            if (questions[y].Score == questions[x].Score)
                return CompareId(x, y);
            // Compare Score DESCENDING, i.e. highest is first
            return Nullable.Compare(questions[y].Score, questions[x].Score);
        }

        public int ViewCount(int x, int y)
        {
            if (questions[y].ViewCount == questions[x].ViewCount)
                return CompareId(x, y);
            // Compare ViewCount DESCENDING, i.e. highest/most is first
            return Nullable.Compare(questions[y].ViewCount, questions[x].ViewCount);
        }

        public int AnswerCount(int x, int y)
        {
            if (questions[y].AnswerCount == questions[x].AnswerCount)
                return CompareId(x, y);
            // Compare AnswerCount DESCENDING, i.e. highest/most is first
            return Nullable.Compare(questions[y].AnswerCount, questions[x].AnswerCount);
        }
    }
}
