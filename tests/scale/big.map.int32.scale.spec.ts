import { LoadInt32BigMap, NewInt32BigMap, IMap } from '@/collections/map'
import { Serializer } from '@/io'
import { tmp } from '../util'
import { logg } from '@/log'
import { statSync } from 'fs'

const vs: Serializer<string> = {
  serialize: (v, dst) => {
    dst.writeUTF(v)
  },
  deserialize: (src) => {
    return src.readUTF()
  }
}

const MAX = 5000 * 1000

const LOG_MODULUS = MAX / 100

const roundtrip = (map: IMap<number, string>) => {
  expect(map.size).toBe(MAX)
  for (let i = 0; i < MAX; i++) {
    const exp = `${i}`
    const found = map.get(i)

    expect(found).not.toBeNull()
    expect(found).toBe(exp)

    if (i && !(i % LOG_MODULUS)) {
      logg(`Read so far: ${i}`)
    }
  }
}

describe('Test Storage', () => {
  const map = NewInt32BigMap<string>(4, vs, { tableSize: 4999, timestamps: true })
  it('Will put', () => {
    for (let i = 0; i < MAX; i++) {
      const prev = map.put(i, `${i}`, true)

      expect(prev).toBeNull()

      const found = map.get(i)

      expect(found).not.toBeNull()
      expect(found).toBe(`${i}`)

      if (i && !(i % LOG_MODULUS)) {
        logg(`Written so far: ${i}`)
      }
    }
    logg(`Finished writing ${MAX} entries. Image Size: ${map.imageSize}`)
  })

  it('Will get', () => {
    roundtrip(map)
  })

  it('Will stream', () => {
    let i = 0

    for (const { key, value } of map.entries()) {
      expect(value).toBe(`${key}`)
      i++
      if (!(i % LOG_MODULUS)) {
        logg(`Streamed so far: ${i}`)
      }
    }

    expect(i).toBe(MAX)
  })

  it('Will keep working after cloning', () => {
    logg(`Serializing ${map.imageSize} bytes`)
    const buffer = map.serialize()
    const copy = LoadInt32BigMap(buffer, vs)
    roundtrip(copy)
  })

  it('Will keep working after persisting', () => {
    const temp = tmp()
    map.saveOn(temp)

    expect(map.imageSize).toBe(statSync(temp).size)

    const copy = LoadInt32BigMap(temp, vs)
    roundtrip(copy)
  })
})
