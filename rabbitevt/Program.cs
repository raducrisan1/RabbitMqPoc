using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace rabbitevt
{
    class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Send();    
            }
            Receive();
        }

        private static void Send()
        {
            var factory = new ConnectionFactory {HostName = "localhost"};
            using (var con = factory.CreateConnection())
            using (var model = con.CreateModel())
            {
                IBasicProperties props = model.CreateBasicProperties();
                props.Expiration = "3600000";
                
                model.QueueDeclare(queue: "test", durable: true, exclusive: false, autoDelete: false, arguments: null);
                string msg = "my msg";
                var body = Encoding.UTF8.GetBytes(msg);
                model.BasicPublish(exchange: "", routingKey: "test", basicProperties:props, body:body);
                Console.WriteLine($"The message '{msg}' has been sent");
            }
        }

        private static void Receive()
        {
            AutoResetEvent sync = new AutoResetEvent(false);
            var factory = new ConnectionFactory {HostName = "localhost"};
            using (var con = factory.CreateConnection())
            using (var ch = con.CreateModel())
            {
                ch.QueueDeclare(queue: "test", durable: true, exclusive: false, autoDelete: false, arguments: null);
                var consumer = new EventingBasicConsumer(ch);
                consumer.Received += (sender, args) =>
                {
                    var body = args.Body;
                    var msg = Encoding.UTF8.GetString(body);
                    Console.WriteLine($"Received {msg}");
                    ch.BasicAck(args.DeliveryTag, multiple: false);
                    sync.Set();
                };
                ch.BasicConsume(queue: "test", autoAck: false, consumer: consumer);
                sync.WaitOne();
            }
        }
    }
}