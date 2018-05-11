import { KafkaConsumer, ConsumerStreamMessage } from "node-rdkafka";
import bunyan from "bunyan";

export default class PromiseConsumer {
    private consumer: KafkaConsumer;
    private logger = bunyan.createLogger({ name: "PromiseConsumer" });

    constructor(kafkaConfig: any, topicConfig: any) {
        this.consumer = new KafkaConsumer(kafkaConfig, topicConfig);
    }

    public connect(timeout: number = 1000): Promise<void> {
        const readyPromise = new Promise<void>((resolve, reject) => {
            const timeoutTrigger = setTimeout(() => {
                this.logger.error("Silence respond on connect.");
                reject();
            }, timeout);

            this.consumer.once("ready", () => {
                this.logger.info("Consumer is ready");
                clearTimeout(timeoutTrigger);
                resolve();
            });
        });

        this.consumer.connect(null);
        return readyPromise;
    }

    public subscribe(topics: string[]) {
        this.consumer.subscribe(topics);
    }

    public consume(maxMessage: number = 1): Promise<ConsumerStreamMessage[]> {
        return new Promise((resolve, reject) => {
            this.consumer.consume(maxMessage, (err: Error, messages: ConsumerStreamMessage[]) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(messages);
                }
            });
        });
    }

    public commit(): void {
        this.consumer.commit(null);
    }

    public disconnect(timeout: number = 1000): Promise<any> {
        const disconnectPromise = new Promise<any>((resolve, reject) => {
            const timeoutTrigger = setTimeout(() => {
                this.logger.error("Silence respond on disconnect.");
                reject();
            }, timeout);

            this.consumer.disconnect((err: any, info: any) => {
                if (err) {
                    this.logger.error("Cannot disconnect with ", err);
                    reject(err);
                } else {
                    this.logger.info("Disconnected.");
                    clearTimeout(timeoutTrigger);
                    resolve(info);
                }
            });
        });

        return disconnectPromise;
    }
}