import * as fs from 'fs'
import * as os from 'os'
import { join } from 'path'
import { uuid } from 'uuidv4'
import process from 'process'
import { logg } from '@/log'
import random from 'random'

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

const swap = <T> (array: T[], i: number, j: number) => {
  const tmp = array[i]
  array[i] = array[j]
  array[j] = tmp
}

export const shuffle = <T> (array: T[]) => {
  for (let i = array.length; i > 1; i--) {
    swap(array, i - 1, random.int(0, i - 1))
  }
}

// eslint-disable-next-line generator-star-spacing
export const range = function* (min: number, max: number) {
  while (min < max) {
    yield min++
  }
}

// eslint-disable-next-line generator-star-spacing
export const bigRange = function* (min: bigint, max: bigint) {
  while (min < max) {
    yield min++
  }
}
