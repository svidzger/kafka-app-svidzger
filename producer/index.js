const Kafka = require('node-rdkafka');

console.log('*** Producer starts... ***');

function mainProducer() {
  const stream = Kafka.Producer.createWriteStream(
    {
      'metadata.broker.list': 'localhost:9092',
    },
    {},
    { topic: 'task' }
  );

  function randomizeIntegerBetween(from, to) {
    return Math.floor(Math.random() * (to - from + 1)) + from;
  }

  function queueMessage() {
    const o1 = randomizeIntegerBetween(1, 2);
    const o2 = randomizeIntegerBetween(1, 2);
    const o3 = randomizeIntegerBetween(2, 3);

    let result = false;

    if (o1 + o2 === o3) {
      result = true;
    }

    const message = {
      msg: `Is ${o1} + ${o2} = ${o3} correct?`,
      rslt: result,
    };

    const success = stream.write(Buffer.from(JSON.stringify(message)));

    if (success) {
      console.log('Message successfully to stream');
    } else {
      console.log('Problem writing to stream...');
    }
  }
  setInterval(() => {
    queueMessage();
  }, 2500);
}

mainProducer();
