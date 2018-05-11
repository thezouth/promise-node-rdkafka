import { Producer } from "node-rdkafka";
import bunyan from "bunyan";

import KafkaConnectionError from "./connection-error";

export default class PromiseProducer {
    private producer: Producer;
    private logger = bunyan.createLogger({ "name": "PromiseProducer" });

    constructor(kafkaConfig: any, topicConfig: any, pollInterval: number = 100) {
        kafkaConfig = Object.assign({}, kafkaConfig, { dr_cb: true });
        
        this.producer = new Producer(kafkaConfig, topicConfig);
        this.wrapSendCallback(pollInterval);
    }

    public connect(timeout: number = 3000): Promise<void> {
        const readyPromise = new Promise<void>((resolve, reject) => {
            const timeoutTrigger = setTimeout(() => {
                reject(new KafkaConnectionError("Silence respond on connection."));
            }, timeout);

            this.producer.once("ready", () => {
                this.logger.info("Producer is ready.");
                clearTimeout(timeoutTrigger);
                resolve();
            });
        });

        this.producer.connect(null);
        return readyPromise;
    }

    public produce(topic: string, message: Buffer, 
                   key: Buffer | null = null, partition: number | null = null): Promise<any> {
        return new Promise((resolve, reject) => {
            const timestamp = Date.now();
            const callback = {
                resolve: resolve,
                reject: reject,
            }

            this.producer.produce(topic, partition, message, key, timestamp, 
                { callback: callback });
            this.producer.poll();
        });
    }

    public async disconnect(timeout: number = 10000): Promise<void> {
        try {
            await this.flush(timeout);
        } catch(err) {
            this.logger.error("Error on flush message in Kafka", err);
        }

        await this.disconnectKafka(timeout);
    }

    private flush(timeout: number): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this.producer.flush(timeout, (err: any) => {
                if (err) {
                    reject(err);
                } else {
                    this.logger.debug("All messages is flushed.");
                    resolve();
                }
            });
        });
    }

    private disconnectKafka(timeout: number): Promise<void> {
        const disconnectedPromise = new Promise<void>((resolve, reject) => {
            const timeoutTrigger = setTimeout(() => {
                reject(new KafkaConnectionError("Silence respond on disconnect."));
            }, timeout);

            this.producer.disconnect((err) => { 
                clearTimeout(timeoutTrigger);

                if (err) {
                    this.logger.error("Error on Disconnect", err);
                    reject(err);
                } else {
                    this.logger.info("Disconnected.");
                    resolve(); 
                }
            });
        });

        return disconnectedPromise;
    }

    private wrapSendCallback(pollInterval: number): void {
        this.producer.on("delivery-report", resolveOnDeliveryReport);
        this.producer.setPollInterval(pollInterval);
    }
}


export function resolveOnDeliveryReport(err: any, report: any) {
    if (report.opaque.callback) {
        if (err) {
            report.opaque.callback.reject(err);
        } else {
            report.opaque.callback.resolve(report);
        }
    }
}