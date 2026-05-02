const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'focus-consumer',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'focus-group' });

async function run() {
  await consumer.connect();
  console.log("✅ Consumer connected to Kafka");
  
  await consumer.subscribe({ topic: 'focus-output', fromBeginning: false });

  console.log("🎧 Listening for focus level updates on 'focus-output'...");
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`🎯 Received Result: ${message.value.toString()}`);
    },
  });
}

run().catch(console.error);
