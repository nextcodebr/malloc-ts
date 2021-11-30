/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { GrowableArray } from '@/collections/array'
import { Serializer } from '@/io'
import { logg } from '@/log'
import { tmp } from './util'

export { }

type VO = {
  age: number
  name: string
  siblings?: VO[]
}

const VOSerializer: Serializer<VO> = {
  serialize: function (vo, dst) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this
    dst.writeInt32(vo.age)
    dst.writeUTF(vo.name)
    if (vo.siblings) {
      dst.writeBoolean(true)
      dst.writeStream(vo.siblings, (v, d) => self.serialize(v, d))
    } else {
      dst.writeBoolean(false)
    }
  },
  deserialize: function (src) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this
    const age = src.readInt32()
    const name = src.readUTF()
    const sib = src.readBoolean()

    let siblings

    if (sib) {
      siblings = src.readStream(s => self.deserialize(s))
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

const roundtrip = (array: GrowableArray<VO>, max: number) => {
  expect(array.length).toBe(max)

  for (let i = 0; i < max; i++) {
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
}

describe('Test Array (Coverage)', () => {
  it('Will fail if huge', () => {
    expect(() => {
      try {
        return new GrowableArray(256 * 1024 * 1024, VOSerializer)
      } catch (e) {
        throw new Error(((e as any).message as string) ?? '')
      }
    }).toThrow(Error)

    expect(() => {
      try {
        return new GrowableArray(512 * 1024 * 1024, VOSerializer, 0)
      } catch (e) {
        throw new Error(((e as any).message as string) ?? '')
      }
    }).toThrow(Error)
  })

  const array = new GrowableArray(16, VOSerializer)

  expect(array.pop()).toBeUndefined()

  array.push(mock(0))
  expect(array.length).toBe(1)
  const pop = array.pop()!
  expect(array.length).toBe(0)
  expect(pop.name).toBe(mock(0).name)
  expect(pop.age).toBe(mock(0).age)
  expect(array.pop()).toBeUndefined()
  expect(array.length).toBe(0)

  expect(array.get(-1)).toBeUndefined()
  expect(array.get(array.length)).toBeUndefined()

  const p = array.set(1, mock(1))
  const ps = array.sizeOf(p)
  const q = array.set(1, mock(1))
  expect(q).toBe(p)

  const large = mock(0)
  for (let i = 0; i < 20; i++) {
    large.siblings?.push(mock(i + 1))
  }

  const r = array.set(1, large)
  expect(array.sizeOf(r)).toBeGreaterThan(ps)
})

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
    roundtrip(array, MAX)
  })

  it('Will keep working after cloning', () => {
    const buffer = array.serialize()
    const copy = GrowableArray.load(buffer, VOSerializer)

    roundtrip(copy, MAX)
  })

  it('Will keep working after persisting to disk and loading', () => {
    const temp = tmp('.bin')
    logg(`Created temp file ${temp}`)
    array.saveOn(temp)
    const copy = GrowableArray.load(temp, VOSerializer)

    roundtrip(copy, MAX)
  })
})
