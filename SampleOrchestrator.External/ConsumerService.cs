using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SampleOrchestrator.BLL;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SampleOrchestrator.External
{
    public class ConsumerService : BackgroundService
    {
        private readonly ConsumerConfig _consumerConfig;
        private readonly IConfiguration _config;
        private CancellationTokenSource _cancellationTokenSource;
        private readonly ILogger _logger;
        private readonly IConsumeProcess _nettingPartTwo;

        public ConsumerService(IConfiguration config, ILogger<ConsumerService> logger, IConsumeProcess nettingPartTwo)
        {
            _logger = logger;
            _config = config;
            _consumerConfig = new ConsumerConfig
            {
                BootstrapServers = config.GetValue<string>("Kafka:BootstrapServers"),
                GroupId = config.GetValue<string>("Kafka:GroupId"),
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                AllowAutoCreateTopics = true,
                IsolationLevel = IsolationLevel.ReadCommitted
            };
            _nettingPartTwo = nettingPartTwo;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            var validationPartTwoComplete = _config.GetValue<string>("Topic:ValidationPartTwoComplete");
            //var nettingPartTwoStatus = _config.GetValue<string>("Topic:NettingPartTwoStatus");

            var tasks = new List<Task>
            {
                RegisterTopic(validationPartTwoComplete,_nettingPartTwo,stoppingToken),
                //RegisterTopic(nettingPartTwoStatus,_nettingPartTwoStatusChecker,stoppingToken)
            };

            await Task.WhenAll(tasks.ToArray());
        }

        private Task RegisterTopic(string topic, IConsumeProcess process, CancellationToken stoppingToken)
        {
            return Task.Run(async () =>
            {
                do
                {
                    try
                    {
                        await ConsumeAsync<string>(topic, process.ConsumeAsync, false, _cancellationTokenSource.Token);
                    }
                    catch (Exception e)
                    {
                        _logger.LogCritical(e, $"Error when consuming topic \"{topic}\".");
                    }
                } while (!_cancellationTokenSource.IsCancellationRequested);
            }, stoppingToken);
        }


        private async Task ConsumeAsync<TKey>(string topic, Func<ConsumeResult<TKey, string>, CancellationToken, Task> consumeAsync, bool commitOnError, CancellationToken cancellationToken)
        {
            using (var consumer = new ConsumerBuilder<TKey, string>(_consumerConfig).Build())
            {
                consumer.Subscribe(topic);

                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        ConsumeResult<TKey, string> result = null;

                        try
                        {
                            result = consumer.Consume(cancellationToken);
                        }
                        catch (ConsumeException e)
                        {
                            _logger.LogInformation(e, e.Message);
                        }

                        if (result == null)
                        {
                            continue;
                        }

                        try
                        {
                            _logger.LogInformation(JsonConvert.SerializeObject(result));
                            await consumeAsync(result, cancellationToken);
                            consumer.Commit(result);
                        }
                        catch (OperationCanceledException e)
                        {
                            throw e;
                        }
                        catch (Exception e)
                        {
                            if (!commitOnError)
                            {
                                throw e;
                            }
                            consumer.Commit(result);
                        }
                    }
                }
                catch (OperationCanceledException e)
                {
                    _logger.LogWarning(e, $"Stopped consuming topic \"{topic}\".");
                }
                finally
                {
                    consumer.Close();
                }
            }
        }
    }
}
