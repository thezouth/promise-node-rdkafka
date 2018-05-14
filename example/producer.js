const kafkaClient = require("../dist/index.js");
const PromiseProducer = kafkaClient.PromiseProducer;

if (process.argv.length < 3) {
    console.log("Usage: node producer.js message [topic] [bootstrap.server]")
    process.exit(1);
}

const message = process.argv[2];
const topic = process.argv.length >= 4 ? process.argv[3] : "test-topic";
const bootstrapServer = process.argv.length >= 5 ? process.argv[4] : "localhost:9092";

console.log("Produce message `" + message + "` to " + topic + "@" + bootstrapServer);

const producer = new PromiseProducer({"bootstrap.servers": bootstrapServer}, {});

(async () => {
    try {
        await producer.connect();
        console.log("Producer connected.");
        producer.produce(topic, Buffer.from(message));
        producer.produce(topic, Buffer.from(message));
        await producer.disconnect(2000);
        console.log("Producing done.");
    } catch (err) {
        console.log("Error", err);
        process.exit(1);
    }
    
})();
