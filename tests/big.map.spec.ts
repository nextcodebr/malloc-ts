import { NewBigMap, LoadBigMap, IMap } from '@/collections/map'
import { Serializer } from '@/io'
import { tmp } from './util'

const ks: Serializer<number> = {
  serialize: (v, dst) => {
    dst.writeInt32(v)
  },
  deserialize: (src) => src.readInt32()
}

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
  const map = NewBigMap<number, string>(4, v => v, ks, vs, { tableSize: 4999, timestamps: true })

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
    const copy = LoadBigMap(buffer, v => v, ks, vs)
    roundtrip(copy)
  })

  it('Will keep working after persisting', () => {
    const temp = tmp()
    map.saveOn(temp)
    const copy = LoadBigMap(temp, v => v, ks, vs)
    roundtrip(copy)
  })
})
