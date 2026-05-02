const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'learning-engine-backend',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'backend-consumer-group' });

const initKafka = async () => {
  try {
    await producer.connect();
    console.log('Kafka Producer connected');
    
    await consumer.connect();
    console.log('Kafka Consumer connected');
  } catch (err) {
    console.error('Error connecting to Kafka:', err);
  }
};

const sendActivityEvent = async (event) => {
  try {
    await producer.send({
      topic: 'user_activity',
      messages: [
        { value: JSON.stringify(event) },
      ],
    });
  } catch (err) {
    console.error('Error sending activity event:', err);
  }
};

module.exports = { kafka, producer, consumer, initKafka, sendActivityEvent };
