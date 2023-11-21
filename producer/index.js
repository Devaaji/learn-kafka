const { Kafka, Partitioners } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});

const produceMessage = async () => {
  await producer.connect();
  await producer.send({
    topic: 'example-topic',
    messages: [
      { value: 'Hello Deva 1!' },
    ],
  });
  await producer.disconnect();
};

produceMessage();
