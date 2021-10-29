using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SampleOrchestrator.BLL.Redis
{
    public interface IRedisService
    {
        Task SaveAsync(string key, object value);
        Task AddToSetAsync(string key, string value);
        Task<T> GetAsync<T>(string key);
        Task<string[]> GetSetMembersAsync(string key);
        Task<bool> DeleteAsync(string key);
        Task<bool> DeleteByPatternAsync(string pattern);
        Task AddToSetAsync(string key, RedisValue[] value);
        Task SaveBatchAsync(KeyValuePair<RedisKey, RedisValue>[] data);
    }
}
