import { LoadInt32BigMap, NewInt32BigMap, BufferUnderflowError } from '@/collections/map'
import { Serializer } from '@/io'
import { shuffle, range } from './util'

const vs: Serializer<string> = {
  serialize: (v, dst) => {
    dst.writeUTF(v)
  },
  deserialize: (src) => {
    return src.readUTF()
  }
}

const LargeZeros = Buffer.alloc(256).fill(48).toString('utf8')
const LargeOnes = Buffer.alloc(512).fill(49).toString('utf8')

describe('Test Map Ops', () => {
  const map = NewInt32BigMap<string>(1, vs, { tableSize: 1, timestamps: true })

  it('Will put and replace (small)', () => {
    let found = map.put(0, 'foo', true)
    expect(found).toBeNull()

    found = map.get(0)
    expect(found).toBe('foo')

    found = map.put(0, 'bar', true)
    expect(found).toBe('foo')

    found = map.put(0, 'baz', false)
    expect(found).toBeNull()

    found = map.get(0)
    expect(found).toBe('baz')

    found = map.remove(0, true)
    expect(found).toBe('baz')

    found = map.get(0)
    expect(found).toBeNull()

    found = map.put(0, 'baz', true)
    expect(found).toBeNull()
    found = map.get(0)
    expect(found).toBe('baz')
  })

  it('Will put and replace (large)', () => {
    map.clear()
    expect(map.size).toBe(0)

    let found = map.put(0, 'foo', true)
    expect(found).toBeNull()

    found = map.get(0)
    expect(found).toBe('foo')

    found = map.put(0, LargeZeros, true)
    expect(found).toBe('foo')
    found = map.get(0)
    expect(found).toBe(LargeZeros)

    found = map.put(0, LargeOnes, true)
    expect(found).toBe(LargeZeros)
    found = map.get(0)
    expect(found).toBe(LargeOnes)
  })

  it('Sequential insert/shuffled delete', () => {
    map.clear()
    expect(map.size).toBe(0)

    const keys = [...range(0, 5000)]

    for (const key of keys) {
      map.put(key, `${key}`)
    }

    expect(map.size).toBe(keys.length)

    shuffle(keys)

    for (const key of keys) {
      const prev = map.remove(key, true)
      expect(prev).toBe(`${key}`)
    }
  })

  it('Shuffled insert/Sequential delete', () => {
    map.clear()
    expect(map.size).toBe(0)

    const keys = [...range(0, 5000)]
    const shuffled = [...keys]
    shuffle(shuffled)

    for (const key of shuffled) {
      map.put(key, `${key}`)
    }

    expect(map.size).toBe(keys.length)

    for (const key of keys) {
      const prev = map.remove(key, true)
      expect(prev).toBe(`${key}`)
    }
  })

  it('Shuffled insert/Shuffled delete', () => {
    map.clear()
    expect(map.size).toBe(0)

    const keys = [...range(0, 5000)]
    shuffle(keys)
    const shuffled = [...keys]
    shuffle(shuffled)

    for (const key of shuffled) {
      map.put(key, `${key}`)
    }

    expect(map.size).toBe(keys.length)

    for (const key of keys) {
      const prev = map.remove(key, true)
      expect(prev).toBe(`${key}`)
    }
  })

  it('Shuffled insert/Sequential iterate', () => {
    map.clear()
    expect(map.size).toBe(0)

    const keys = [...range(0, 5000)]
    const shuffled = [...keys]
    shuffle(shuffled)

    for (const key of shuffled) {
      map.put(key, `${key}`)
    }

    expect(map.size).toBe(keys.length)

    let ix = 0

    for (const key of map.keys()) {
      expect(key).toBe(keys[ix++])
    }
    expect(ix).toBe(keys.length)

    ix = 0

    for (const val of map.values()) {
      expect(val).toBe(`${keys[ix++]}`)
    }
    expect(ix).toBe(keys.length)
  })

  it('Will underflow on wrong sized buffer', () => {
    map.clear()
    expect(map.size).toBe(0)

    const keys = [...range(0, 100)]

    for (const key of keys) {
      map.put(key, `${key}`)
    }

    const buffer = map.serialize()

    expect(() => LoadInt32BigMap(buffer.slice(0, buffer.byteLength / 2), vs)).toThrowError(BufferUnderflowError)
  })

  it('Cant Insert null keys', () => {
    map.clear()
    expect(map.size).toBe(0)

    expect(() => map.put((undefined as unknown) as number, 'foo')).toThrowError('Key cannot be null')
  })

  it('Will put only if absent', () => {
    map.clear()
    expect(map.size).toBe(0)

    let found = map.put(0, 'foo', true)

    found = map.putIfAbsent(0, 'bar', true)
    expect(found).toBe('foo')
    expect(map.get(0)).toBe('foo')

    found = map.put(0, 'bar', true)
    expect(found).toBe('foo')
    expect(map.get(0)).toBe('bar')

    found = map.putIfAbsent(1, 'baz', true)
    expect(found).toBe(null)
    found = map.get(1)
    expect(found).toBe('baz')

    map.clear()
    expect(map.size).toBe(0)

    found = map.putIfAbsent(0, 'bar', true)
    expect(found).toBe(null)
    found = map.get(0)
    expect(found).toBe('bar')
  })

  it('Will compute if absent', () => {
    map.clear()
    expect(map.size).toBe(0)

    let found = map.computeIfAbsent(0, k => `${k}`)

    expect(found).toBe('0')

    found = map.computeIfAbsent(0, k => 'Wrong')
    expect(found).toBe(null)

    found = map.computeIfAbsent(0, k => 'Wrong', true)
    expect(found).toBe('0')
  })

  it('Remove is noop for absent keys', () => {
    map.clear()
    expect(map.size).toBe(0)

    expect(map.remove(0, true)).toBe(null)
  })
})