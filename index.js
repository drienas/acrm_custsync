if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

const rmq = require('./rmq');
const axios = require('axios');

let iConnection, iChannel, iQueue;

const ELASTIC_URL = process.env.ELASTIC_URL || null;

if (!ELASTIC_URL) {
  console.log(`No Elastic-URL found`);
  process.exit(0);
}

const ElasticIndex = `${ELASTIC_URL}/acrm_custsync/_doc`;

const ack = (msg) => iChannel.ack(msg, false);
const nackError = (msg) => iChannel.nack(msg, false, true);
const nack = (msg) => iChannel.nack(msg, false, false);

const validateMessage = (msg) => {
  let cnt = JSON.parse(msg.content.toString());
  if (cnt && cnt.id) {
    if (Array.isArray(cnt)) {
      nack(msg);
      return null;
    }
    return cnt;
  }
};

const consumeQueue = (channel, queue) => {
  console.log(`Listening to customer syncs from DSG Backend...`);
  channel.consume(
    queue.queue,
    async (msg) => {
      console.log('\n');
      try {
        const cnt = validateMessage(msg);
        if (!cnt) return;
        let id = cnt.id;
        delete cnt.id;
        try {
          console.log(`Handling ${id}`);
          await axios.delete(`${ElasticIndex}/${id}`);
        } catch (err) {
          console.log(`Deleting existing dataset...`);
          if (
            err.response &&
            err.response.data &&
            err.response.data.status === 404
          )
            console.log(`Nothing to delete...`);
        } finally {
          console.log(`Creating new dataset...`);
          await axios.post(`${ElasticIndex}/${id}`, cnt);
          console.log(`Done.`);
          ack(msg);
        }
      } catch (err) {
        console.error(err);
        nackError(msg);
      }
    },
    { noAck: false }
  );
};

(async () => {
  iConnection = await rmq.connect();
  iChannel = await rmq.initExchangeChannel(iConnection, 'autocrm_customer_x');

  iQueue = await rmq.initQueue(
    iChannel,
    'autocrm_customer_x',
    'ACRM_CustomerBridge',
    ['update.dsg.detail'],
    {
      prefetch: true,
      prefetchCount: 1,
    }
  );

  consumeQueue(iChannel, iQueue);
})();
