using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using SampleOrchestrator.BLL.Dto;
using SampleOrchestrator.BLL.Kafka;
using System;
using System.Threading.Tasks;

namespace SampleOrchestrator.FrontendApi.Controllers
{
    [ApiController]
    [Route("sample")]
    public class SampleController
    {
        private readonly IKafkaSender _sender;
        private readonly ILogger _logger;
        private readonly IConfiguration _config;

        public SampleController(IKafkaSender sender, ILogger<SampleController> logger, IConfiguration config)
        {
            _config = config;
            _sender = sender;
            _logger = logger;
        }

        [HttpPost]
        [Route("start")]
        [ProducesResponseType(typeof(string), 200)]
        [ProducesResponseType(typeof(string), 400)]
        public async Task<ActionResult> Start()
        {
            var validationPartTwo = _config.GetValue<string>("Topic:validationPartTwo");
            try
            {
                var validation = new ValidationDto()
                {
                    ContractId = Guid.NewGuid().ToString(),
                    Timestamp = DateTime.UtcNow,
                    Metadata = new BaseMessageDto()
                    {
                        Activity = "validationPartTwo",
                        Status = BLL.Enum.EnumStatus.Begin,
                        TrxId = Guid.NewGuid(),
                        Timestamp = DateTime.UtcNow
                    }
                };

                await _sender.SendAsync(validationPartTwo, validation);

                return new OkResult();
            }
            catch (Exception e)
            {
                _logger.LogError(e.ToString());
                return new BadRequestResult();
            }
        }
    }
}
