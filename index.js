(async _ => {

  const DEBUG = false
  const apiPort = process.env.APIPORT || 4444
  const express = require('express')
  const app = express()
  const cors = require('cors')

  const broker = '192.168.67.33'

  const { exec } = require('child_process')
  const execute = (cmd, callback) => exec(cmd, (_, stdout) => callback(stdout))

  app.use(cors())
  app.use(express.json())

  const api = async (req, res) => {

    res.setHeader('Content-Type', 'application/json')
    let topic, partition, offset
    if (req.method === 'POST') {
      topic = req.body.topic
      partition = req.body.partition
      offset = req.body.offset
    } 
    if (req.method === 'GET') {
      topic = req.params.topic
      partition = req.params.partition
      offset = req.params.offset
    }
    console.log(req.method, topic, partition, offset)
    const data = await new Promise((resolve, reject) => {
      try {
        const cmd = `kafkacat -C -b ${broker} -t ${topic} -p ${partition} -o ${offset} -c 1 -e -q`
        execute(cmd, result => resolve(result))
      } catch (err) {
        console.log(err.message)
        reject({})
      }
    })
    res.end(data)
  }

  app.get('/api/v1/:topic/:offset', api)
  app.post('/api/v1/kafka', api)

  app.listen(apiPort, _ => 
    console.log('Offsetter at port', apiPort)
  ).on('error', err => console.log(err.message))

})()
