using System;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace ServiceBusWithFilter
{
    class Program
    {
        private const string Topicname = "filter-poc";
        private const string ConnectionString = "Endpoint=sb://[YOUR_SERVICE_BUS]/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=[YOUR_SERVICE_BUS_KEY]";
        static void Main(string[] args)
        {
            // Prepare ServiceBus Topic
            var topic = TopicClient.CreateFromConnectionString(ConnectionString, Topicname);
            var ns = NamespaceManager.CreateFromConnectionString(ConnectionString);

            // Create a "HighMessages" filtered subscription.
            var sf1 = new SqlFilter("FilterAnyName = 1");
            if (!ns.SubscriptionExists(Topicname, "Subscription1"))
                ns.CreateSubscription(Topicname, "Subscription1", sf1);

            var sf2 = new SqlFilter("FilterAnyName = 2");
            if (!ns.SubscriptionExists(Topicname, "Subscription2"))
                ns.CreateSubscription(Topicname, "Subscription2", sf2);
            
            // Subscription 1
            var subs1 = SubscriptionClient.CreateFromConnectionString(ConnectionString, Topicname, "Subscription1");
            subs1.OnMessage(message =>
            {
                var body = message.GetBody<string>();
                Console.WriteLine(body);
            });

            // Subscription 2
            var subs2 = SubscriptionClient.CreateFromConnectionString(ConnectionString, Topicname, "Subscription2");
            subs2.OnMessage(message =>
            {
                var body = message.GetBody<string>();
                Console.WriteLine(body);
            });

            // Send
            var filterAnyName = 0;
            for (var t = 1; t <= 10; t++)
            {
                var ms = new BrokeredMessage($"Test message {t}");
                filterAnyName = (t % 2 == 0) ? 2 : 1;
                ms.Properties["FilterAnyName"] = filterAnyName;
                topic.Send(ms);
            }

            Console.ReadKey();
        }
    }
}