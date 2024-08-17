using DataTransferApp.Services;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

namespace DataTransferApp.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class DataTransferController : ControllerBase
    {
        private readonly DataTransferService _dataTransferService;

        public DataTransferController(DataTransferService dataTransferService)
        {
            _dataTransferService = dataTransferService;
        }

        [HttpPost("transfer")]
        public async Task<IActionResult> TransferData()
        {
            await _dataTransferService.TransferDataAsync();
            return Ok("Data transfer initiated.");
        }
    }
}
