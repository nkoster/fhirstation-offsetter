(async _ => {

  const DEBUG = true
  const express = require('express')
  const app = express()
  const cors = require('cors')

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
