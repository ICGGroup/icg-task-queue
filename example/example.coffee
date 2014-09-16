TaskQueue = require("../task-queue")
bunyan = require('bunyan');

logOptions =
  name: 'example-log'
  streams: [
    stream: process.stdout
    level: "debug"
  ]

log = bunyan.createLogger(logOptions)


taskQueue = new TaskQueue
  secToken: "f0a4d82d-d2a1-4e83-9efb-84482d5806b0"
  taskResourceUrl: "http://localhost:3000/api/ap/queuedTasks"
  log: log

# ask for work
processOpts =
  backoff:
    algorithm  : 'fibonacci',
    delayRatio : 1,
    maxDelay   : 300,
    maxTries: 1000000
  log: log


processQ = taskQueue.process "non.notification", processOpts, (task, cb)->
  log.debug "starting processing for task", task
