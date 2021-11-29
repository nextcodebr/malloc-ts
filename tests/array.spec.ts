/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { GrowableArray } from '@/collections/array'
import { Serializer } from '@/io'

type VO = {
  age: number
  name: string
  siblings?: VO[]
}

const VOSerializer: Serializer<VO> = {
  serialize: function (vo, dst) {
    dst.writeInt32(vo.age)
    dst.writeUTF(vo.name)
    if (vo.siblings) {
      dst.writeBoolean(true)
      dst.writeStream(vo.siblings, this.serialize)
    } else {
      dst.writeBoolean(false)
    }
  },
  deserialize: function (src) {
    const age = src.readInt32()
    const name = src.readUTF()
    const sib = src.readBoolean()

    let siblings

    if (sib) {
      siblings = src.readStream(this.deserialize)
    }

    return {
      age,
      name,
      siblings
    }
  }
}

const mock = (i: number) => {
  const vo: VO = {
    age: i,
    name: `John Armless#${i}`
  }

  if ((i & 1) === 0) {
    vo.siblings = [{
      age: i + 1,
      name: `Mary the Worker#${i}`
    }]
  }
  return vo
}

describe('Test Storage', () => {
  const array = new GrowableArray(16, VOSerializer)
  const MAX = 1000

  it('Will Store correctly', () => {
    for (let i = 0; i < MAX; i++) {
      const vo = mock(i)

      array.set(i, vo)
    }

    expect(array.length).toBe(MAX)
  })

  it('Will Load collectly', () => {
    expect(array.length).toBe(MAX)

    for (let i = 0; i < MAX; i++) {
      const exp = mock(i)

      const found = array.get(i)!
      expect(found).not.toBeUndefined()

      expect(found.age).toBe(exp.age)
      expect(found.name).toBe(exp.name)

      if (exp.siblings) {
        for (let i = 0; i < exp.siblings.length; i++) {
          const s = found.siblings![i]
          expect(s.age).toBe(exp.siblings[i].age)
          expect(s.name).toBe(exp.siblings[i].name)
        }
      } else {
        expect(found.siblings).toBeUndefined()
      }
    }
  })
})
