import {
    connect,
    delay,
    Empty,
    nuid,
    StorageType,
    Events,
    AckPolicy,
    ReplayPolicy,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/main/src/mod.ts";

// create an admin connection and setup stream and consumer
const admin = await connect({servers: "localhost:4222", noAsyncTraces: true});
const jsm = await admin.jetstreamManager();
const name = nuid.next();
await jsm.streams.add({ name, storage: StorageType.File, subjects: [`${name}.*`], num_replicas: 3 });
await jsm.consumers.add(name, { durable_name: name, ack_policy: AckPolicy.Explicit, replay_policy: ReplayPolicy.Instant, num_replicas: 3 });
await admin.drain();
// start the producer
const p = producer();
// start the consumer
const c = consumer();
// wait for either to exit
await Promise.race([p, c]);

async function producer(): Promise<void> {
    // connect to the server
    const nc = await connect({servers: "localhost:4222", noAsyncTraces: true});
    // print connection events
    (async () => {
        for await(const s of nc.status()) {
            switch(s.type) {
                case Events.Disconnect:
                case Events.Reconnect:
                    console.log("<", s);
                    break;
                default:
                // nothing
            }
        }
    })().then();

    // publish events on the root subject - expecting that our internal sequence is correct always
    const js = nc.jetstream();
    let seq = 0;
    while(true) {
        seq++;
        const subj = `${name}.${seq}`;
        // we have to keep trying to publish, as we may miss acks
        let reattempts = 0;
        while(true) {
            try {
                // if the pa succeeded, this means we expect the data to be in the raft logs for the cluster
                const pa = await js.publish(subj, Empty, { msgID: `${seq}` });
                if(pa.seq !== seq) {
                    // in chaos this can fail, if the followers are not caught up, the sequence will be off
                    // but shouldn't happen
                    console.log(`expected ${seq} got ${pa.seq}`, pa);
                    Deno.exit(1);
                }
                break;
            } catch(err) {
                // if we fail, we retry up-to 1000 times
                reattempts++;
                if(reattempts >= 1000) {
                    console.log(`failed to publish ${seq} ${err.message} - giving up`);
                    Deno.exit(1);
                }
                console.log(`failed to publish ${seq} ${err.message} - reattempting ${reattempts}`);
                await delay(100);
            }
        }
    }
}

async function consumer(): Promise<void> {
    // connect
    const nc = await connect({servers: "localhost:4222", noAsyncTraces: true});
    // print connect/disconnect events
    (async () => {
        for await(const s of nc.status()) {
            switch(s.type) {
                case Events.Disconnect:
                case Events.Reconnect:
                    console.log(">", s);
                    break;
                default:
                // nothing
            }
        }
    })().then();

    // consumer must exist - this is the only thing that will stop the consumer from starting
    const c = await nc.jetstream().consumers.get(name, name);
    // we start a consume
    const iter = await c.consume({max_messages: 100});
    // the consume will print status events these could be useful
    const status = await iter.status();
    (async () => {
        for await(const s of status) {
            // skip the `next` events - as these are notified everytime we pull
            if(s.type !== "next") {
                console.log(s);
            }
        }
    })().then()
    for await(const m of iter) {
        // we print the sequence number, and ack the message, the >> will denote a dupe
        m.info.redelivered ? console.log(">>", m.seq) : console.log(">", m.seq);
        await m.ack();
    }
}




