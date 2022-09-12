import amqplib from 'amqplib';

async function sleep(ms, cb) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  })
  .then(() => {
    cb && cb()
  })
}

let _instance = null;

class MessageBroker {
  constructor(url, keys, cb) {
    this._url = url;
    this._initCallback = cb;
    this._channels = {};
    this._connection = null;
    this._exchange = keys.exchange,
    this._queue = keys.task;
  }

  get con() {
    return this._connection;
  }

  get chan() {
    return this._channels;
  }

  async init(instance, props={ connect:true, publisher:true, consumer:true }) {
    try {
      _instance = instance;
      if (props.connect) {
        this._connection = await _instance.connect();
        console.log('MessageBroker.init successfully established.');
      }
      if (props.publisher) {
        this._channels.publisher = await this._connection.createChannel();
      }
      if (props.consumer) {
        this._channels.consumer = await this._connection.createChannel();
      }
      this._initCallback && await this._initCallback(_instance);
      
      this._channels.publisher.on('close', async (err) => {
        _instance.onClose('publisher', 3, err, {
          connect: false,
          publisher: true,
          consumer: false
        })
      })

      this._channels.publisher.on('close', async (err) => {
        _instance.onClose('consumer', 3, err, {
          connect: false,
          publisher: false,
          consumer: true
        })
      })

    } catch (error) {
      console.log('MessageBroker.init', error)
    }
  }

  async onClose(type, sec, err, initProps) {
    console.log(`MessageBroker.onClose ${type} closed unexpectedly.`, { err });
    console.log(`MessageBroker.onClose Reestablish in ${sec} seconds...`, { err });

    sleep(sec * 1000, async () => {
      await _instance.init(
        _instance,
        initProps
      )
    })
  }

  async connect() {
    try {
      const connection = await amqplib.connect(this._url);

      connection.on('error', (err) => {
        console.error('MessageBroker.connect error', { err })
      })
  
      connection.on('close', async (err) => {
        _instance.onClose('connection', 10, err)
      })

      return connection
    } catch (error) {
      console.log('MessageBroker.connect', { error });
    }
  }

  async consume(queue, exchange, routingKey, handler) {
    try {
      const ch = await this._connection.createChannel();

      ch.on('error', (err) => {
        console.log('MessageBroker.consume channel error.', { err });
      });
  
      await ch
        .assertQueue(this._queue[queue], { exclusive: false })
        .then(async () => {
          await ch
            .bindQueue(
              this._queue[queue],
              this._exchange[exchange],
              routingKey
            )
        })
        .then(async () => {
          await ch
            .consume(
              this._queue[queue],
              async (msg) => {
                handler(_instance, msg);
                await ch.close()
              },
              { noAck: true }
            );
        })
    } catch (error) {
      throw error
    }
  }

  async basicConsume({exchange, task, taskById}, handler) {
    try {
      await _instance.chan.consumer
      .assertQueue(this._queue[task], { exclusive: false })
      .then(async () => {
        await _instance.chan.consumer
          .bindQueue(
            this._queue[task],
            this._exchange[exchange],
            taskById || task
          )
      })
      .then(async () => {
        await _instance.chan.consumer
          .consume(
            this._queue[task],
            async (msg) => {
              const { isSuccess } = await handler(_instance, msg);
              console.log('MessageBroker.basicConsume', { isSuccess });
              _instance.chan.consumer.ack(msg);
              if (isSuccess === undefined) {
                console.error('MessageBroker.basicConsume isSuccess must be defined.', { msg });
              } else if (!isSuccess) {
                // TODO: Store error somewhere with the msg data
              }
            },
            { noAck: false }
          )
      })
    } catch (error) {
      console.log('MessageBroker.basicConsume', { error });
    }
  }

  async basicPublish(exchange, routingKey, msg) {
    try {
      const payload = JSON.stringify(msg);
      return await _instance.chan.publisher
        .assertExchange(this._exchange[exchange], 'direct', {
          durable: false
        })
        .then(async () => {
          await _instance.chan.publisher
            .publish(
              this._exchange[exchange],
              routingKey,
              Buffer.from(payload)
            );
          return true;
        })
    } catch (error) {
      throw error
    }
  }

  async registerIllegalOperationErrorHandler(ch, method, closed, consume) {
    // Experimental
    // Handle throws IllegalOperationError if channel is closed
    // and replace channel

    ch.newMethod = ch[method];
    ch[method] = (data) => {
      !closed && ch.newMethod(msg);
      sleep(10000, async () => {
        console.log('IllegalOperationError');
        const ch = await consume();
        await ch[method](data)
      })
    }
  }
}

export default MessageBroker;