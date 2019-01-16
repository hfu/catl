const config = require('config')
const fs = require('fs')
const zlib = require('zlib')
const byline = require('byline')
const parser = require('json-text-sequence').parser
const Queue = require('better-queue')
const { spawn } = require('child_process')
const TimeFormat = require('hh-mm-ss')
const pretty = require('prettysize')
const modify = require('./modify.js')

const srcDir = config.get('srcDir')
const dstDir = config.get('dstDir')

const queue = new Queue((t, cb) => {
  const startTime = new Date()
  const srcPath = `${t.srcDir}/${t.file}`
  const dstPath = `${dstDir}/${t.file.replace('ndjson.gz', 'mbtiles')}`
  const partPath = 
    `${dstDir}/part-${t.file.replace('ndjson.gz', 'mbtiles')}`
  fs.stat(srcPath, (err, srcStat) => {
    if (err) throw err
    t.size = srcStat.size
    if (fs.existsSync(dstPath)) {
      fs.stat(dstPath, (err, dstStat) => {
        if (err) throw err
        if (dstStat.mtime > srcStat.mtime) {
          console.log(`${dstPath} is newer. Skipped.`)
          return cb()
        } else {
          console.log(`${t.file}: ${pretty(srcStat.size)}`)
        }
      })
    } else {
      console.log(`${t.file}: ${pretty(srcStat.size)}`)
    }
  })
  const p = new parser()
  .on('json', f => {
    f = modify(f)
console.log(f)
    if (f) {
      if (tippecanoe.stdin.write(`${JSON.stringify(f)}\n`)) {
      } else {
        src.pause()
        tippecanoe.stdin.once('drain', () => {
          src.resume()
        })
      }
    }
  })
  .on('finish', () => {
    tippecanoe.stdin.end(() => {
    })
  })
  .on('truncated', buf => {
    console.log(`truncated: ${buf}`)
  })
  .on('invalid', buf => {
    console.log(`invalid: ${buf}`)
  })
  const tippecanoe = spawn('tippecanoe', [
    '--no-feature-limit', '--no-tile-size-limit', 
    /* '--quiet', */ '-f', '--simplification=2', 
    '--minimum-zoom=14', '--maximum-zoom=14', '--base-zoom=14',
    '-o', `${partPath}`
  ], { stdio: ['pipe', 'inherit', 'inherit'] })
  tippecanoe.on('close', () => {
    fs.rename(partPath, dstPath, err => {
      if (err) throw err
      const stat = queue.getStats()
      console.log(`${stat.total} of ${stat.peak} processed. ${TimeFormat.fromMs(new Date() - startTime)} for ${t.file} (${pretty(t.size)})`)
      return cb()
    })
  })
  const gunzip = zlib.createGunzip()
console.log(gunzip)
  gunzip.pipe(p)
console.log(p)
console.log(srcPath)
  fs.createReadStream(srcPath).pipe(gunzip)
}, { concurrent: config.get('concurrent') })

let count = 0
const ls = spawn('ls', [
  '-S', srcDir
], { stdio: ['inherit', 'pipe', 'inherit'] })
byline(ls.stdout).on('data', line => {
  const file = line.toString()
  if (file.match(/6-.*?ndjson.gz/)) {
count++
if (count > 0) {
    queue.push({
      srcDir: srcDir,
      file: file
    })
}
  } 
})
