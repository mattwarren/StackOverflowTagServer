using Server.Infrastructure;
using System.Linq;
using System.Web.Http;

namespace Server.Controllers
{
    [ActionWebApiFilter]
    public class QuestionsController : ApiController
    {
        [Route("api/Questions/{id}")]
        [HttpGet]
        public Shared.Question Get(int id)
        {
            return WebApiApplication.TagServer.Value.Questions.Where(qu => qu.Id == id).FirstOrDefault();
        }
    }
}
