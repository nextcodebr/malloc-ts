import { NewBigMap } from '@/collections/map'
import { Serializer } from '@/io'

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

describe('Test Storage', () => {
  const map = NewBigMap<number, string>(4, v => v, ks, vs, { tableSize: 4999, timestamps: true })

  it('Will put', () => {
    for (let i = 0; i < 10000; i++) {
      const prev = map.put(i, `${i}`, true)

      expect(prev).toBeNull()

      const found = map.get(i)

      expect(found).not.toBeNull()
      expect(found).toBe(`${i}`)
    }
  })

  it('Will get', () => {
    for (let i = 0; i < 10000; i++) {
      const exp = `${i}`
      const found = map.get(i)

      expect(found).not.toBeNull()
      expect(found).toBe(exp)
    }
  })
})
