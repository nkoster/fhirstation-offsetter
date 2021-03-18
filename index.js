(async _ => {

  const DEBUG = true
  const express = require('express')
  const app = express()
  const cors = require('cors')

  const { Kafka } = require('kafkajs')

  const kafka = new Kafka({
    clientId: 'offsetter',
    brokers: ['pvdevkafka01:9092']
  })

  const consumer = kafka.consumer({ groupId: 'test-group' })

  await consumer.connect()
  await consumer.subscribe({ topic: 'fhir4.capybara.firefly.medicom.observation', offset: '20139091' })

  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: true,
    eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
        uncommittedOffsets,
        isRunning,
        isStale,
    }) => {
        if (isStale()) {
          return
        }
        for (let message of batch.messages) {
            console.log({
                topic: batch.topic,
                partition: batch.partition,
                highWatermark: batch.highWatermark,
                message: {
                    offset: message.offset,
                    key: message.key.toString(),
                    value: message.value.toString(),
                    headers: message.headers,
                }
            })
            consumer.pause()
            break;
            // resolveOffset(message.offset)
            // await heartbeat()
        }
    },
  })
  
  await consumer.seek({ topic: 'fhir4.capybara.firefly.medicom.observation', partition: 0, offset: 20139091 })
  
  const consoleLog = (...args) => {
    process.stdout.write(`${new Date().toUTCString()}:`)
    args.forEach(arg => process.stdout.write(` ${arg.toString()}`))
    process.stdout.write('\n')
  }

  const apiPort = process.env.APIPORT || 4444

  app.use(cors())
  app.use(express.json())

  const api = async (req, res) => {
    consoleLog(req.body)
  }

  app.post('/api/v1/:topic/:offset', api)

  app.listen(apiPort, _ => 
    consoleLog('Offsetter at port', apiPort)
  ).on('error', err => consoleLog(err.message))

})()
