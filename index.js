const config = require('config')
const fs = require('fs')
const zlib = require('zlib')
const byline = require('byline')
const Queue = require('better-queue')
const { spawn } = require('child_process')
const TimeFormat = require('hh-mm-ss')
const pretty = require('prettysize')
const modify = require('./modify.js')

const srcDir = config.get('srcDir')
const dstDir = config.get('dstDir')

const queue = new Queue((t, cb) => {
  const srcPath = `${t.srcDir}/${t.file}`
  fs.stat(srcPath, (err, stat) => {
    if (err) throw err
    console.log(`${srcPath}: ${pretty(stat.size)}`)
  })
  const src = byline(fs.createReadStream(srcPath)
    .pipe(zlib.createGunzip()))
  const tippecanoe = spawn('tippecanoe', [
    '--no-feature-limit', '--no-tile-size-limit', 
    /*'--quiet',*/ '-f', '--simplification=2', '-o', 
    `${dstDir}/${t.file.replace('ndjson.gz', 'mbtiles')}`
  ], { stdio: ['pipe', 'inherit', 'inherit'] })
  src.on('data', line => {
    if (line.length === 0) return
    let f
    try {
      f = modify(JSON.parse(line))
    } catch (e) {
      console.log(`input error (${srcPath}): ${line}`)
      // console.log(e.stack)
      // console.log('resuming anyway.')
      return
    }
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
  src.on('end', () => {
    tippecanoe.stdin.end(() => {
    })
  })
  tippecanoe.on('close', () => {
    const stat = queue.getStats()
    console.log(`${stat.total} of ${stat.peak} processed. Ave ${TimeFormat.fromMs(stat.average)} Est ${TimeFormat.fromMs(stat.average * stat.peak)}`)
    return cb()
  })
}, { concurrent: config.get('concurrent') })

const ls = spawn('ls', [
  '-S', srcDir
], { stdio: ['inherit', 'pipe', 'inherit'] })
byline(ls.stdout).on('data', line => {
  const file = line.toString()
  if (file.match('6-')) {
    queue.push({
      srcDir: srcDir,
      file: file
    })
  } 
})
