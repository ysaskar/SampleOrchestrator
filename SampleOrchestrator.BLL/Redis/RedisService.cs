using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SampleOrchestrator.BLL.Redis
{
    public class RedisService : IRedisService
    {

        private readonly ILogger _logger;

        private readonly Lazy<ConnectionMultiplexer> _lazyConnection;

        public RedisService(IConfiguration configuration, ILogger<RedisService> logger)
        {
            var connectionString = configuration.GetValue<string>("RedisServer:ConnectionString");
            var config = ConfigurationOptions.Parse(connectionString);
            config.ConnectTimeout = 10_000;
            config.AsyncTimeout = 10_000;
            config.SyncTimeout = 10_000;
            _lazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(config));
            _logger = logger;
        }

        public ConnectionMultiplexer Connection => _lazyConnection.Value;

        private IDatabase _redisDb => Connection.GetDatabase();

        private List<IServer> _redisServers
        {
            get
            {
                var endpoints = Connection.GetEndPoints();
                var server = new List<IServer>();

                foreach (var endpoint in endpoints)
                {
                    server.Add(Connection.GetServer(endpoint));
                }

                return server;
            }
        }

        public async Task<bool> DeleteAsync(string key)
        {
            try
            {
                var stringValue = await _redisDb.KeyDeleteAsync(key);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return false;
            }
        }

        public async Task<bool> DeleteByPatternAsync(string pattern)
        {
            try
            {
                foreach (var redisServer in _redisServers)
                {
                    var keys = redisServer.Keys(pattern: pattern).ToArray();
                    await _redisDb.KeyDeleteAsync(keys);
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return false;
            }
        }

        public async Task<T> GetAsync<T>(string key)
        {
            try
            {
                var stringValue = await _redisDb.StringGetAsync(key);
                if (string.IsNullOrEmpty(stringValue))
                {
                    return default(T);
                }
                else
                {
                    var objectValue = JsonConvert.DeserializeObject<T>(stringValue);
                    return objectValue;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return default(T);
            }
        }

        public async Task SaveAsync(string key, object value)
        {
            try
            {
                var stringValue = JsonConvert.SerializeObject(value,
                    new JsonSerializerSettings()
                    {
                        ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                    });
                await _redisDb.StringSetAsync(key, stringValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return;
            }
        }

        public async Task SaveBatchAsync(KeyValuePair<RedisKey, RedisValue>[] data)
        {
            try
            {
                await _redisDb.StringSetAsync(data);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return;
            }
        }


        public async Task<string[]> GetSetMembersAsync(string key)
        {
            try
            {
                var setMember = await _redisDb.SetMembersAsync(key);
                if (null != setMember)
                {
                    return setMember.ToStringArray();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
            return null;
        }

        public async Task AddToSetAsync(string key, string value)
        {
            try
            {
                await _redisDb.SetAddAsync(key, value);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return;
            }
        }

        public async Task AddToSetAsync(string key, RedisValue[] value)
        {
            try
            {
                await _redisDb.SetAddAsync(key, value);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return;
            }
        }
    }
}
