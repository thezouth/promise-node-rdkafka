import PromiseProducer from "../promise-producer";
import { resolveOnDeliveryReport } from "../promise-producer";
import { Producer } from "node-rdkafka";

jest.mock("node-rdkafka");

describe("PromiseProducer", () => {
    beforeEach(() => {
        Producer.mockClear();
    });

    it("Initial with mandatory option", () => {
        const myProducer = new PromiseProducer({}, {});
        expect(myProducer).toBeTruthy();
        expect(Producer).toHaveBeenCalledTimes(1);
        expect(Producer.mock.calls[0][0].dr_cb).toBeTruthy();

        const producerInstance = Producer.mock.instances[0];
        expect(producerInstance.setPollInterval).toHaveBeenCalled();
    });

    describe("connect", () => {
        it("successful connect", (done) => {
            const myProducer = new PromiseProducer({}, {});
            const connectPromise = myProducer.connect(50);

            const producerInstance = Producer.mock.instances[0];
            const readyListener = producerInstance.once.mock.calls[0];
            expect(readyListener[0]).toBe("ready");
            readyListener[1]();
            
            connectPromise.then(() => done());
        }, 55);

        it("no respond from Kafka", (done) => {
            const myProducer = new PromiseProducer({}, {});
            myProducer.connect(50).catch(() => {
                done()
            });
        }, 55);
    })

    describe("produce", () => {
        it("Produce result as given parameter", async () => {
            const myProducer = new PromiseProducer({}, {});
            const message = Buffer.from("message");
            const key = Buffer.from("key");
    
            const promise = myProducer.produce("test-topic", message, key);
    
            const producerInstance = Producer.mock.instances[0];
            expect(producerInstance.produce).toHaveBeenCalled();
            const params = producerInstance.produce.mock.calls[0];
            expect(params[0]).toEqual("test-topic");
            expect(params[2]).toEqual(message);
            expect(params[3]).toEqual(key);
            
            runCallback(params);
            await promise;
        });

        function runCallback(producerParams: any) {
            resolveOnDeliveryReport(null, { opaque: { callback: producerParams[5].callback }});
        }
    })
});