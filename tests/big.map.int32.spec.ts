import { NewBigInt32Map } from '@/collections/map'
import { Serializer } from '@/io'

const vs: Serializer<string> = {
  serialize: (v, dst) => {
    dst.writeUTF(v)
  },
  deserialize: (src) => {
    return src.readUTF()
  }
}

describe('Test Storage', () => {
  const map = NewBigInt32Map<string>(4, vs, { tableSize: 4999, timestamps: true })

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
