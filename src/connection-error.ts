export default class KafkaConnectionError extends Error {
    constructor(message: string) {
        super(message);
    }
}