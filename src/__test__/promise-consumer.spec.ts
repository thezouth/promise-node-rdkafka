import PromiseConsumer from "../promise-consumer";
import { KafkaConsumer } from "node-rdkafka";
import bunyan from "bunyan";

jest.mock("node-rdkafka");

describe("PromiseConsumer", () => {
    let myConsumer: PromiseConsumer;
    let nativeInstance;

    beforeEach(() => {
       initiateMock();
    });

    it("Initiate KafkaConsumer", () => {
        expect(myConsumer).toBeTruthy();
        expect(KafkaConsumer).toHaveBeenCalledTimes(1);
    });

    describe("connect", () => {
        it("successful connect", (done) => {
            const connectPromise = myConsumer.connect(50);
            const readyListener = nativeInstance.once.mock.calls[0];
            expect(readyListener[0]).toBe("ready");
            readyListener[1]();
            
            connectPromise.then(() => done());
        }, 55);

        it("no respond from Kafka, should throw error.", (done) => {
            myConsumer.connect(50).catch(() => {
                done()
            });
        }, 55);
    });

    describe("consume", () => {
        it("successful consume message ", (done) => {
            const maxMessage = 10;
            myConsumer.consume(maxMessage).then(() => {
                done();
            }).catch((err) => {
                done.fail("Consume error.");
            });
            
            expect(nativeInstance.consume).toHaveBeenCalled();
            const consumeParams = nativeInstance.consume.mock.calls[0];
            expect(consumeParams[0]).toEqual(maxMessage);
            consumeParams[1](null, []);

        }, 50);

        it("failed on consume", (done) => {
            const maxMessage = 10;
            myConsumer.consume(maxMessage).then(() => {
                done.fail("Fail on native consume should not return as pass.")
            }).catch((err) => {
                done();
            });
            
            expect(nativeInstance.consume).toHaveBeenCalled();
            const consumeParams = nativeInstance.consume.mock.calls[0];
            expect(consumeParams[0]).toEqual(maxMessage);
            consumeParams[1](new Error("Error"));
        }, 50);
    });

    describe("disconnect", () => {
        it("successful disconnect", (done) => {
            myConsumer.disconnect(50)
                .then(() => done())
                .catch((err) => done.fail(err));

            expect(nativeInstance.disconnect).toHaveBeenCalled();
            nativeInstance.disconnect.mock.calls[0][0]();
        }, 55);

        it("no respond from Kafka, should throw error.", (done) => {
            myConsumer.disconnect(50).catch(() => {
                done()
            });
        }, 55);

        it("failure on disconnect", (done) => {
            myConsumer.disconnect(50)
                .then(() => done.fail("Fail on native disconnect should not return as pass."))
                .catch((err) => done());

            expect(nativeInstance.disconnect).toHaveBeenCalled();
            nativeInstance.disconnect.mock.calls[0][0](new Error("error"));

        })
    });

    function initiateMock() {
        KafkaConsumer.mockClear();
        myConsumer = new PromiseConsumer({}, {});
        nativeInstance = KafkaConsumer.mock.instances[0];

        nativeInstance.consume.mockClear();
        nativeInstance.connect.mockClear();
        nativeInstance.disconnect.mockClear();
        nativeInstance.once.mockClear();
        nativeInstance.on.mockClear();
    }

});