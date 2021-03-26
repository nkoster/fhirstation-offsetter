(async _ => {

  const DEBUG = false
  const apiPort = process.env.APIPORT || 4444
  const express = require('express')
  const app = express()
  const cors = require('cors')
  const { Kafka } = require('kafkajs')

  const kafka = new Kafka({
    clientId: 'offsetter',
    brokers: ['pvdevkafka01:9092']
  })

  const consumer = kafka.consumer({ groupId: 'offsetter' })

  const consoleLog = (...args) => {
    process.stdout.write(`${new Date().toUTCString()}:`)
    args.forEach(arg => process.stdout.write(` ${arg?.toString()}`))
    process.stdout.write('\n')
  }

  app.use(cors())
  app.use(express.json())

  const api = async (req, res) => {
    try {
      res.setHeader('Content-Type', 'application/json')
      console.log(req.method)
      let topic, offset
      if (req.method === 'POST') {
        topic = req.body.topic
        offset = req.body.offset
      } 
      if (req.method === 'GET') {
        topic = req.params.topic
        offset = req.params.offset
      }
      DEBUG && consoleLog('GET KAFKA', topic, offset)
      try {
        await consumer.disconnect()
        await consumer.connect()
        await consumer.subscribe({ topic, fromBeginning: true })      
      } catch(err) {
        consoleLog(err)
      }
      let kafkaMessage = {}
      let partition = 0
      try {
        await consumer.run({
          autoCommit: false,
          eachBatchAutoResolve: true,
          eachBatch: async ({ batch, isStale }) => {
            if (isStale()) {
              return
            }
            for (let message of batch.messages) {
              kafkaMessage = {
                topic: batch.topic,
                partition: batch.partition,
                highWatermark: batch.highWatermark,
                message: {
                  offset: message.offset,
                  key: message.key.toString(),
                  value: message.value.toString(),
                  headers: message.headers
                }
              }
              partition = batch.partition
              DEBUG && consoleLog('OFFSET', kafkaMessage)
              consumer.pause([{ topic: batch.topic, partitions: [batch.partition] }])
              consumer.disconnect()
              break
            }
            res.end(JSON.stringify(kafkaMessage))
          }
        })
        consumer.seek({ topic, partition, offset })
      } catch(err) {
        consoleLog(err)
        res.end({ 'error': err.message })
      }
    } catch(err) {
      consoleLog(err)
      res.end({ 'catched': err.message })
    }
  }

  app.get('/api/v1/:topic/:offset', api)
  app.post('/api/v1/kafka', api)

  app.listen(apiPort, _ => 
    consoleLog('Offsetter at port', apiPort)
  ).on('error', err => consoleLog(err.message))

})()
