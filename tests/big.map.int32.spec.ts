import { LoadInt32BigMap, NewInt32BigMap, IMap } from '@/collections/map'
import { Serializer } from '@/io'
import { tmp } from './util'

const vs: Serializer<string> = {
  serialize: (v, dst) => {
    dst.writeUTF(v)
  },
  deserialize: (src) => {
    return src.readUTF()
  }
}

const MAX = 10000

const roundtrip = (map: IMap<number, string>) => {
  expect(map.size).toBe(MAX)
  for (let i = 0; i < MAX; i++) {
    const exp = `${i}`
    const found = map.get(i)

    expect(found).not.toBeNull()
    expect(found).toBe(exp)
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
    }
  })

  it('Will get', () => {
    roundtrip(map)
  })

  it('Will stream', () => {
    let count = 0

    for (const { key, value } of map.entries()) {
      expect(value).toBe(`${key}`)
      count++
    }

    expect(count).toBe(MAX)
  })

  it('Will keep working after cloning', () => {
    const buffer = map.serialize()
    const copy = LoadInt32BigMap(buffer, vs)
    roundtrip(copy)
  })

  it('Will keep working after persisting', () => {
    const temp = tmp()
    map.saveOn(temp)
    const copy = LoadInt32BigMap(temp, vs)
    roundtrip(copy)
  })
})
