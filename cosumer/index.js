const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'my-group' });

const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'example-topic', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
    },
  });
};

runConsumer().catch(e => console.error(`[Consumer] ${e.message}`, e));

// Keep the script running to continue consuming messages
process.on('SIGTERM', async () => {
  await consumer.disconnect();
  process.exit();
});

process.on('SIGINT', async () => {
  await consumer.disconnect();
  process.exit();
});
