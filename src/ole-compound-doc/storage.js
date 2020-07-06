//---------//
// Imports //
//---------//

const es = require('event-stream')

//
//------//
// Main //
//------//

class Storage {
  constructor(doc, dirEntry) {
    this._doc = doc
    this._dirEntry = dirEntry
  }

  storage(storageName) {
    return new Storage(this._doc, this._dirEntry.storages[storageName])
  }

  streamFromBuffer(streamName) {
    const streamEntry = this._dirEntry.streams[streamName] || this._dirEntry.streams[`${streamName}\u0000`]
    if (!streamEntry) return null

    const doc = this._doc
    let bytes = streamEntry.size

    let allocationTable = doc._SAT
    let shortStream = false
    if (bytes < doc._header.shortStreamMax) {
      shortStream = true
      allocationTable = doc._SSAT
    }

    const secIds = allocationTable.getSecIdChain(streamEntry.secId)

    return es.readable(function(i, callback) {
      const stream = this // function called in context of stream

      if (i >= secIds.length) {
        stream.emit('end')
        return
      }

      let buffer = shortStream
        ? doc._readOneShortSectorFromBuffer(secIds[i])
        : doc._readOneSectorFromBuffer(secIds[i])

      if (bytes - buffer.length < 0) {
        buffer = buffer.slice(0, bytes)
      }

      bytes -= buffer.length
      stream.emit('data', buffer)
      callback()
    })
  }

  streamFromFile(streamName) {
    const streamEntry = this._dirEntry.streams[streamName]
    if (!streamEntry) return null

    const doc = this._doc
    let bytes = streamEntry.size

    let allocationTable = doc._SAT
    let shortStream = false
    if (bytes < doc._header.shortStreamMax) {
      shortStream = true
      allocationTable = doc._SSAT
    }

    const secIds = allocationTable.getSecIdChain(streamEntry.secId)

    return es.readable(function(i, callback) {
      const stream = this // function called in context of stream

      if (i >= secIds.length) {
        stream.emit('end')
        return
      }

      if (shortStream) {
        doc._readOneShortSectorFromFile(secIds[i], sectorCallback)
      } else {
        doc._readOneSectorFromFile(secIds[i], sectorCallback)
      }

      return

      // scoped helper functions

      function sectorCallback(buffer) {
        if (bytes - buffer.length < 0) {
          buffer = buffer.slice(0, bytes)
        }

        bytes -= buffer.length
        stream.emit('data', buffer)
        callback()
      }
    })
  }
}

//
//---------//
// Exports //
//---------//

module.exports = Storage
