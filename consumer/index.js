const Kafka = require('node-rdkafka');

console.log('*** Consumer starts... ***');

function mainConsumer() {
  //Main consumer
  const consumer = Kafka.KafkaConsumer(
    {
      'group.id': 'kafka',
      'metadata.broker.list': 'localhost:9092',
    },
    {}
  );

  let message = '';
  let result = '';

  consumer.connect();
  consumer
    .on('ready', () => {
      console.log('consumer ready...');
      consumer.subscribe(['task']);
      consumer.consume();
    })
    .on('data', (data) => {
      let jsonData = JSON.parse(data.value);

      for (key in jsonData) {
        if (key === 'msg') {
          message = jsonData[key];
        }
        if (key === 'rslt') {
          result = jsonData[key];
        }
      }
      console.log(`received message: ${message}`);
      console.log(`result is:  ${result}`);
    });
}

mainConsumer();
