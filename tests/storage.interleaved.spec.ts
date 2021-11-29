import { NewStorage } from '@/malloc'
import { logg } from '@/log'
import random from 'random'

const swap = <T> (array: T[], i: number, j: number) => {
  const tmp = array[i]
  array[i] = array[j]
  array[j] = tmp
}

const shuffle = <T> (array: T[]) => {
  for (let i = array.length; i > 1; i--) {
    swap(array, i - 1, random.int(0, i - 1))
  }
}

const update = (v: { req: { min: number, max: number }, actual: { min: number, max: number } }, req: number, sz: number) => {
  if (req > v.req.max) {
    v.req.max = req
  }
  if (sz > v.actual.max) {
    v.actual.max = sz
  }

  if (req < v.req.min) {
    v.req.min = req
  }

  if (sz < v.actual.min) {
    v.actual.min = sz
  }
}

describe('Test Allocate and Free Chunks of random size', () => {
  const MAX = 10000
  const stats = {
    requested: 0,
    allocated: 0,
    allocs: 0,
    frees: 0,
    batches: 0,
    req: { min: Number.MAX_SAFE_INTEGER, max: 0 },
    actual: { min: Number.MAX_SAFE_INTEGER, max: 0 }
  }
  const storage = NewStorage(0)
  const offsets: number[] = []

  let batch = 100 + random.int(0, 20)
  it('Will Allocate and Free Shuffled', () => {
    for (let i = 0; i < MAX; i++) {
      const size = random.int(0, 1024 * 512)

      stats.requested += size

      const p = storage.allocate(size)

      expect(p).toBeGreaterThan(0)

      const sz = storage.sizeOf(p)

      expect(sz).toBeGreaterThanOrEqual(size)

      stats.allocated += sz
      update(stats, size, sz)

      stats.allocs++
      offsets.push(p)
      if ((i % batch) === 0) {
        stats.batches++
        shuffle(offsets)

        let maxToFree = random.int(0, offsets.length - 1)

        while (maxToFree) {
          const off = offsets.pop()
          if (!off) {
            throw new Error('Popped null')
          }
          const res = storage.free(off)
          expect(res).toBeTruthy()
          maxToFree--
          stats.frees++
        }

        batch = 100 + random.int(0, 20)
      }
    }

    let off = offsets.pop()
    while (off) {
      const res = storage.free(off)
      expect(res).toBeTruthy()
      off = offsets.pop()

      stats.frees++
    }
    stats.batches++

    logg(`${JSON.stringify(stats)}`)
  })
})
