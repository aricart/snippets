// nsc add operator O --sys
// nsc edit operator --account-jwt-server-url nats://localhost:4222  --service-url nats://localhost:4222
// nsc generate config --nats-resolver --config-dir /tmp/config --sys-account SYS --config-file /tmp/server.conf
// nats-server -js -c /tmp/server.conf
// nsc add account A
// nsc edit account A --js-mem-storage 20000 --js-disk-storage 20000 --js-tier 0
// nsc push -A
// nsc add user u
// note the location of the creds

import {
  connect,
  credsAuthenticator,
  StreamConfig,
} from "https://deno.land/x/nats@v1.10.2/src/mod.ts";

const creds = await Deno.readFile(
  "/Users/aricart/.local/share/nats/nsc/keys/creds/O/A/u.creds",
);
const authenticator = credsAuthenticator(creds);

const nc = await connect({ authenticator });
console.log(nc.info);
setInterval(async () => {
  try {
    const jsm = await nc.jetstreamManager();
    const info = await jsm.getAccountInfo();
    console.log(info);
    const config = { name: "S", subjects: ["S"] } as Partial<StreamConfig>;
    if (info?.tiers?.R1) {
      config.num_replicas = 1;
    }

    await jsm.streams.add(config);
    await jsm.streams.delete("S");
  } catch (err) {
    console.log(`error: ${err.message}`);
  }
}, 1000);
