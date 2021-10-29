using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SampleOrchestrator.BLL.Dto;
using SampleOrchestrator.BLL.Enum;
using SampleOrchestrator.BLL.Kafka;
using SampleOrchestrator.BLL.Redis;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SampleOrchestrator.BLL
{
    public class PropagateNettingPartTwo : IConsumeProcess
    {
        private readonly ILogger _logger;
        private readonly IRedisService _redis;
        private readonly IKafkaSender _sender;
        private readonly IConfiguration _config;
        private readonly IRedisLogService _redisLog;

        public PropagateNettingPartTwo(ILogger<PropagateNettingPartTwo> logger, IConfiguration config, IRedisService redis, IKafkaSender sender, IRedisLogService redisLog)
        {
            _logger = logger;
            _redis = redis;
            _sender = sender;
            _config = config;
            _redisLog = redisLog;
        }


        public async Task ConsumeAsync<TKey>(ConsumeResult<TKey, string> consumeResult, CancellationToken cancellationToken = default)
        {
            var nettingPartTwoTopic = _config.GetValue<string>("Topic:NettingPartTwo");

            var msgDto = JsonConvert.DeserializeObject<BaseMessageDto>(consumeResult.Message.Value);
            var trxId = msgDto.TrxId;
            var activity = msgDto.Activity;

            var nettingPartTwo = "NettingPartTwo";

            var logKey = $"LOG_{activity}_{trxId}";
            var failedKey = $"FAILED_{nettingPartTwo}_{trxId}";
            var completedKey = $"COMPLETED_{nettingPartTwo}_{trxId}";
            var sourceKey = $"SOURCE_{nettingPartTwo}_{trxId}";

            var begin = DateTime.UtcNow;
            try
            {

                var logTrx = await _redis.GetAsync<BaseMessageDto>(logKey);

                if (logTrx != null && !logTrx.Status.Equals(EnumStatus.Error))
                {
                    return;
                }

                await _redisLog.LogBegin(msgDto, logKey, begin);


                await _redis.DeleteAsync(failedKey);
                await _redis.DeleteAsync(completedKey);//kayanya ga butuh

                _logger.LogInformation($"Begin propagating netting part two");

                /*
                   insert seed data for neeting logic here
                */

                var taskList = new List<Task>();
                var soureTrx = new List<RedisValue>();

                for (var i = 0; i < 10; i++)
                {
                    for (var j = 0; j < 20; j++)
                    {
                        var temp = new NettingDto()
                        {
                            ContractId = $"Contract {i}",
                            InvestorId = $"Investor {j}",
                            Price = (i + j) * 10,
                            Timestamp = DateTime.UtcNow,
                            TradeFeedId = $"TradeFeed {i}-{j}",
                            Metadata = new BaseMessageDto()
                            {
                                Activity = nettingPartTwo,
                                TrxId = trxId,
                                Status = EnumStatus.Begin,
                                Timestamp = DateTime.UtcNow
                            }
                        };

                        soureTrx.Add(new RedisValue(temp.ContractId + temp.InvestorId));
                        taskList.Add(_sender.SendAsync(nettingPartTwoTopic, temp));
                    }
                }

                taskList.Add(_redis.AddToSetAsync(sourceKey, soureTrx.ToArray()));
                await Task.WhenAll(taskList);

                var success = true;

                if (!success)
                {
                    var msg = $"reason why it failed";
                    var logfailed = _redisLog.LogFailed(msgDto, logKey, msg);
                    var failed = SendMessage(msgDto, EnumStatus.Failed);
                    var registerFailedTrxId = _redis.AddToSetAsync(failedKey, trxId.ToString());

                    await Task.WhenAll(logfailed, failed, registerFailedTrxId);
                    return;
                }

                _logger.LogInformation($"Done propagate with trxId {trxId} in {(DateTime.UtcNow - begin).TotalMilliseconds} milis");

                //save to db if necesary

                var logFinish = _redisLog.LogFinish(msgDto, logKey);

                var complete = SendMessage(msgDto, EnumStatus.Completed);

                await Task.WhenAll(logFinish, complete, logFinish, complete);
            }
            catch (Exception ex)
            {
                var logError = _redisLog.LogError(msgDto, logKey, ex);
                var failed = SendMessage(msgDto, EnumStatus.Error);
                var registerFailedTrxId = _redis.AddToSetAsync(failedKey, trxId.ToString());
                await Task.WhenAll(logError, failed, registerFailedTrxId);
                _logger.LogError(ex, "Error occured on trxId : {id} and step : {step} with error message : {ex}", trxId, msgDto.Activity, ex.ToString());
            }
        }

        private async Task SendMessage(BaseMessageDto dto, EnumStatus status)
        {
            var topic = _config.GetValue<string>("Topic:PropagateNettingPartTwoStatus");
            var msg = new BaseMessageDto()
            {
                TrxId = dto.TrxId,
                Activity = dto.Activity,
                Timestamp = DateTime.UtcNow,
                Status = status
            };
            await _sender.SendAsync(topic, msg);
        }
    }
}
