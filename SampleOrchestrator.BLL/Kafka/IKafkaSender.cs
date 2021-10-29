using System.Threading.Tasks;

namespace SampleOrchestrator.BLL.Kafka
{
    public interface IKafkaSender
    {
        Task SendAsync(string topic, object message);
    }
}
