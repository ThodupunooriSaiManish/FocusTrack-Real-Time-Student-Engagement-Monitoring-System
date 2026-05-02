const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'focus-producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

async function run() {
  await producer.connect();
  console.log("✅ Producer connected to Kafka");

  const departments = ["CSE", "ECE", "MECH", "CIVIL", "IT"];
  const years = [1, 2, 3, 4];
  const sections = ["A", "B"];
  const studentsPerSection = 60;

  setInterval(async () => {
    const events = [];
    for (let i = 0; i < 10; i++) { // Send 10 events at once for better coverage
      const dept = departments[Math.floor(Math.random() * departments.length)];
      const year = years[Math.floor(Math.random() * years.length)];
      const sec = sections[Math.floor(Math.random() * sections.length)];
      const rollNo = Math.floor(Math.random() * studentsPerSection) + 1;
      
      events.push({
        value: JSON.stringify({
          user_id: `${dept}_${year}_${sec}_${rollNo}`,
          department: dept,
          year: year,
          section: sec,
          mentor_id: `mentor_${dept}_${year}_${sec}`,
          time_spent: Math.floor(Math.random() * 110) + 10, // 10-120
          correct: Math.random() < 0.7 // 70% chance correct
        })
      });
    }

    await producer.send({
      topic: 'focus-events',
      messages: events,
    });
    console.log(`📤 Sent ${events.length} events`);
  }, 1000); // every second
}

run().catch(console.error);
