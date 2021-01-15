const crypto = require('crypto')
const {Storage} = require('@google-cloud/storage')
const storage = new Storage()

class Rodeo {
  constructor(cnf) {
    this.cnf = cnf
    this.db = cnf.database

    this.path = this.path.bind(this)

    this.init()
  }

  async init() {
    this.bucket = storage.bucket(this.cnf.bucket)

    let e = await this.bucket.exists()
    if (!e[0]) { await storage.createBucket(this.cnf.bucket, this.cnf.bucketOpts || {}) }

    return
  }

  hash(key) {
    let hash = crypto.createHash('md5').update(key).digest('hex')

    hash = hash.substr(0, 32)
    hash = hash.padStart(32 - hash.length, 0)

    return hash
  }

  file(key) {
    return this.bucket.file(this.path(key))
  }

  set(key, val) {
    let file = this.file(key)
    let content = typeof val == 'object' ? JSON.stringify(val) : val

    return file.save(content, {resumable: false})
  }

  get(keys) {
    let results = []
    keys = typeof keys !== 'object' ? [keys] : keys

    for (let key of keys) {
      let file = this.file(key)
      results.push(file.download())
    }

    return Promise.all(results).then(res => {
      return JSON.parse(res[0][0].toString())
    })
  }

  async exists(key) {
    let file = this.file(key)
    let exists = await file.exists()

    return exists[0]
  }

  path(key) {
    return this.db + '/' + this.hash(key)
  }
}

module.exports = Rodeo
