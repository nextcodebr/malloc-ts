import * as fs from 'fs'
import * as os from 'os'
import { join } from 'path'
import { uuid } from 'uuidv4'
import process from 'process'
import { logg } from '@/log'

const files: string[] = []

process.on('exit', () => {
  for (const file of files) {
    logg(`Removing ${file}`)
    fs.unlinkSync(file)
  }
})

export const tmp = (extension = '') => {
  const rv = join(os.tmpdir(), uuid() + extension)

  files.push(rv)

  return rv
}
