using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace StackOverflowTagServer.DataStructures
{
    // From http://ayende.com/blog/890/the-ultimate-disposable
    internal class DisposableAction : IDisposable
    {
        Action _action;

        public DisposableAction(Action action)
        {
            if (action == null)
                throw new ArgumentNullException("action");
            _action = action;
        }

        public void Dispose()
        {
            _action();
        }
    }
}
