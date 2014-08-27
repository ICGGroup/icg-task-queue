http = require("http")
url = require("url")
path = require("path")
_ = require("lodash")
async = require("async")
oi = require("oibackoff")

saveTask = (taskResourceUrl, task, secToken, callback)->

  taskUrl = url.parse(taskResourceUrl)

  headers =
    'secToken': secToken
    'Content-Type': 'application/json'
    'Accept': 'application/json'

  options =
    headers: headers
    hostname: taskUrl.hostname
    port: taskUrl.port
    path: taskUrl.path
    method: if task.rpmRId then "PUT" else "POST"

  save_request = http.request options, (save_response) ->
    data = ""

    save_response.addListener "data", (chunk) ->
      data += chunk

    save_response.addListener "end", ->
      if callback
        callback(null, data)

    save_response.addListener "error", (error) ->
      if callback
        callback(error)


  save_request.write(JSON.stringify(task))

  save_request.end()

fetchTask = (taskResourceUrl, taskType, secToken, callback)->
  taskUrl = url.parse(taskResourceUrl)
  taskUrl.path = taskUrl.path + "/dequeue/" + taskType

  headers =
    'secToken': secToken
    'Content-Type': 'application/json'
    'Accept': 'application/json'

  options =
    headers: headers
    hostname: taskUrl.hostname
    port: taskUrl.port
    path: taskUrl.path
    method: "GET"

  fetch_request = http.request options, (fetch_response) ->
    data = ""

    fetch_response.addListener "data", (chunk) ->
      data += chunk

    fetch_response.addListener "end", ->
      if callback
        if fetch_response.statusCode is 200
          taskObj = JSON.parse(data)
          if _.isEmpty(taskObj)
            callback("No work to process.")
          else
            callback(null, taskObj)
        else
          callback(data)

    fetch_response.addListener "error", (error) ->
      if callback
        callback(error)

  fetch_request.end()

class Task
  constructor: (taskType, data, taskResourceUrl, secToken, options) ->
    @taskResourceUrl = taskResourceUrl
    @secToken = secToken
    @root =
      progress: 0
      state: "active"
      priority: "normal"
      abandonDelay: 60000
      attempts: 0

    @root.taskType = taskType
    @root.data = data
    if options
      @root = _.extend(@root, options.task)
    @root

  delay: (val)=>
    @root.delay = val
    return @

  priority: (val)=>
    @root.priority = val
    return @

  abandonDelay: (val)=>
    @root.abandonDelay = val
    return @

  attempts: (val)=>
    @root.attempts = val
    return @

  maxAttempts: (val)=>
    @root.maxAttempts = val
    return @

  backoff: (val)=>
    @root.backoff = val
    return @

  backoffMaxDelay: (val)=>
    @root.backoffMaxDelay = val
    return @

  save: (cb)=>
    saveTask @taskResourceUrl, @root, @secToken, cb
    return @

  complete: ()=>
    @root.progress = 100
    @root.state = "complete"
    return @

class TaskQueue
  constructor: (options) ->
    if options

      @taskResourceUrl = options.taskResourceUrl
      @secToken = options.secToken
    if not @taskResourceUrl or not @secToken
      throw new Error("constructor requires taskResourceUrl and secToken options")

  createTask: (taskType, data, options)=>
    task = new Task(taskType, data, @taskResourceUrl, @secToken, options)


  dequeue: (taskType, cb)=>
    fetchTask @taskResourceUrl, taskType, @secToken, cb

  process: (taskType, options, fn)=>
    options ||= {}
    options.concurrency ||= 1
    doFetch = ()=>
      maxDelay = 30
      getWork = (cb)=>
        # console.log(@taskResourceUrl, taskType, @secToken)
        fetchTask @taskResourceUrl, taskType, @secToken, cb

      backoff = oi.backoff
        algorithm  : 'fibonacci',
        delayRatio : 1,
        maxDelay   : maxDelay,
        maxTries: 1000000

      intermediate = (err, tries, delay)->
        if err and err isnt "No work to process."
          true
        else
          try
            if maxDelay < delay
              delay = maxDelay
            # console.log({backoff:'fibonacci', tries: tries, delay:delay})
          catch e
            # console.log(e, "Error calculating intermediate stats")
          finally
            true

      backoff getWork, intermediate, (err, task)->
        asyncQ.push(task)

    asyncQ = async.queue (task, callback)=>
      wrappedTask = new Task(task.taskType, task.taskData, @taskResourceUrl, @secToken, task)
      fn.apply @, [wrappedTask, (err, resultTask)->
        if err
          # set error and save
          wrappedTask.root.error = err.message || err
          wrappedTask.save(callback)
        else
          # complete and save
          wrappedTask.complete().save(callback)
      ]
    , options.concurrency

    asyncQ.empty = doFetch

    doFetch()
    procObj =
      pause: asyncQ.pause
      resume: asyncQ.resume
      kill: asyncQ.kill
      stop: asyncQ.kill
      drain: asyncQ.drain






module.exports = TaskQueue
