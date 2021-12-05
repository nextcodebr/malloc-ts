import { newLength, Sink, Source, JSONSerializer } from '@/io'
import { MAX_SIGNED_32, MIN_SIGNED_32 } from '@/32bit.math'
import { bigRange, range } from './util'

describe('Core IO', () => {
  it('Will fail on negative length', () => {
    expect(() => newLength(-10, 1, 2)).toThrow(Error)
  })

  it(`Will cap request to ${MAX_SIGNED_32 - 8} if current + minGrowth is ${MAX_SIGNED_32 - 8}`, () => {
    expect(newLength(MAX_SIGNED_32 - 16, 8, 20000)).toBe(MAX_SIGNED_32 - 8)
  })

  it('Will use only minGrowth for large requests', () => {
    expect(newLength(MAX_SIGNED_32 - 64, 128, 20000)).toBe(MAX_SIGNED_32 + 64)
  })

  it('Will use only prefGrowth for small requests', () => {
    expect(newLength(256, 128, 20000)).toBe(256 + 20000)
  })
})

describe('Source/Sink Streams', () => {
  const buffer = Buffer.alloc(16)
  const source = new Source(buffer)
  const sink = new Sink(16)

  it('Will read/write booleans', () => {
    sink.writeBoolean(true)
    expect(sink.drainTo(buffer)).toBe(1)
    expect(source.readBoolean()).toBe(true)

    sink.reset().writeBoolean(false)
    expect(sink.drainTo(buffer)).toBe(1)
    expect(source.reset().readBoolean()).toBe(false)
  })

  it('Will read/write uint8', () => {
    sink.reset().writeUInt8(3)
    expect(sink.drainTo(buffer)).toBe(1)
    expect(source.reset().readUInt8()).toBe(3)
  })

  it('Will read/write uint16', () => {
    sink.reset().writeUInt16(30000)
    expect(sink.drainTo(buffer)).toBe(2)
    expect(source.reset().readUInt16()).toBe(30000)
  })

  it('Will read/write uint32', () => {
    sink.reset().writeUInt32(10000000)
    expect(sink.drainTo(buffer)).toBe(4)
    expect(source.reset().readUInt32()).toBe(10000000)
  })

  it('Will read/write uint64', () => {
    sink.reset().writeUInt64(99999999999999n)
    expect(sink.drainTo(buffer)).toBe(8)
    expect(source.reset().readUInt64()).toBe(99999999999999n)
  })

  it('Will read/write int8', () => {
    sink.reset().writeInt8(-(1 << 7))
    expect(sink.drainTo(buffer)).toBe(1)
    expect(source.reset().readInt8()).toBe(-(1 << 7))

    sink.reset().writeInt8(-1)
    expect(sink.drainTo(buffer)).toBe(1)
    expect(source.reset().readInt8()).toBe(-1)

    sink.reset().writeInt8(1)
    expect(sink.drainTo(buffer)).toBe(1)
    expect(source.reset().readInt8()).toBe(1)

    sink.reset().writeInt8((1 << 7) - 1)
    expect(sink.drainTo(buffer)).toBe(1)
    expect(source.reset().readInt8()).toBe((1 << 7) - 1)
  })

  it('Will read/write int16', () => {
    sink.reset().writeInt16(-(1 << 15))
    expect(sink.drainTo(buffer)).toBe(2)
    expect(source.reset().readInt16()).toBe(-(1 << 15))

    sink.reset().writeInt16(-1)
    expect(sink.drainTo(buffer)).toBe(2)
    expect(source.reset().readInt16()).toBe(-1)

    sink.reset().writeInt16(1)
    expect(sink.drainTo(buffer)).toBe(2)
    expect(source.reset().readInt16()).toBe(1)

    sink.reset().writeInt16((1 << 15) - 1)
    expect(sink.drainTo(buffer)).toBe(2)
    expect(source.reset().readInt16()).toBe((1 << 15) - 1)
  })

  it('Will read/write int32', () => {
    expect(1 << 31).toBe(MIN_SIGNED_32)
    sink.reset().writeInt32(1 << 31)
    expect(sink.drainTo(buffer)).toBe(4)
    expect(source.reset().readInt32()).toBe(1 << 31)

    sink.reset().writeInt32(-1)
    expect(sink.drainTo(buffer)).toBe(4)
    expect(source.reset().readInt32()).toBe(-1)

    sink.reset().writeInt32(1)
    expect(sink.drainTo(buffer)).toBe(4)
    expect(source.reset().readInt32()).toBe(1)

    sink.reset().writeInt32(MAX_SIGNED_32)
    expect(sink.drainTo(buffer)).toBe(4)
    expect(source.reset().readInt32()).toBe(MAX_SIGNED_32)
  })

  it('Will read/write int64', () => {
    const min = -(1n << 63n)
    const max = (1n << 63n) - 1n

    expect(min).toBeLessThan(0)
    expect(max).toBeGreaterThan(0)

    sink.reset().writeInt64(min)
    expect(sink.drainTo(buffer)).toBe(8)
    expect(source.reset().readUInt64()).toBe(min)

    sink.reset().writeInt64(-1n)
    expect(sink.drainTo(buffer)).toBe(8)
    expect(source.reset().readUInt64()).toBe(-1n)

    sink.reset().writeInt64(1n)
    expect(sink.drainTo(buffer)).toBe(8)
    expect(source.reset().readUInt64()).toBe(1n)

    sink.reset().writeInt64(max)
    expect(sink.drainTo(buffer)).toBe(8)
    expect(source.reset().readUInt64()).toBe(max)
  })

  it('Will read/write float32', () => {
    sink.reset().writeFloat32(3.1415)
    expect(sink.drainTo(buffer)).toBe(4)
    expect(source.reset().readFloat32().toFixed(4)).toBe('3.1415')
  })
})

describe('Source/Sink Streams Errors', () => {
  const sink = new Sink(16)
  const source = new Source(Buffer.alloc(0))

  const overflow = /.*is out of range.*/
  const negativeOffset = /.*<0.*/

  it('Will fail on uint8 overflow', () => {
    expect(() => sink.reset().writeUInt8(-1)).toThrow(overflow)
    expect(() => sink.reset().writeUInt8(256)).toThrow(overflow)
  })

  it('Will fail on negative offset access', () => {
    expect(() => source.reset().getInt32(-1)).toThrow(negativeOffset)
  })
})

describe('Arrays', () => {
  const buffer = Buffer.alloc(4 * 1024 * 1024)
  const source = new Source(buffer)
  const sink = new Sink(16)

  it('Will write/read uint8 arrays', () => {
    const values = [...range(0, 256)]

    sink.reset().writeUInt8Array(values)
    expect(sink.drainTo(buffer)).toBe(4 + 256)
    const rec = source.reset().readUInt8Array()
    expect([...rec]).toStrictEqual(values)
  })

  it('Will write/read int8 arrays', () => {
    const values = [...range(-128, 128)]

    sink.reset().writeInt8Array(values)
    expect(sink.drainTo(buffer)).toBe(4 + 256)
    const rec = source.reset().readInt8Array()
    expect([...rec]).toStrictEqual(values)
  })

  it('Will write/read uint16 arrays', () => {
    const values = [...range(0, 1 << 16)]

    sink.reset().writeUInt16Array(values)
    expect(sink.drainTo(buffer)).toBe(4 + (1 << 16) * 2)
    const rec = source.reset().readUInt16Array()
    expect([...rec]).toStrictEqual(values)
  })

  it('Will write/read int16 arrays', () => {
    const values = [...range(-(1 << 15), 1 << 15)]

    sink.reset().writeInt16Array(values)
    expect(sink.drainTo(buffer)).toBe(4 + (1 << 16) * 2)
    const rec = source.reset().readInt16Array()
    expect([...rec]).toStrictEqual(values)
  })

  it('Will write/read uint32 arrays', () => {
    const values = [...range(0, 1 << 18)]

    sink.reset().writeUInt32Array(values)
    expect(sink.drainTo(buffer)).toBe(4 + (1 << 18) * 4)
    const rec = source.reset().readUInt32Array()
    expect([...rec]).toStrictEqual(values)
  })

  it('Will write/read int32 arrays', () => {
    const values = [...range(-(1 << 17), 1 << 17)]

    sink.reset().writeInt32Array(values)
    expect(sink.drainTo(buffer)).toBe(4 + (1 << 18) * 4)
    const rec = source.reset().readInt32Array()
    expect([...rec]).toStrictEqual(values)
  })

  it('Will write/read uint64 arrays', () => {
    const values = [...bigRange(0n, 1n << 18n), 1n << 40n]

    sink.reset().writeUInt64Array(values)
    const alignment = 8
    expect(sink.drainTo(buffer)).toBe(alignment + (1 * 8) + ((1 << 18) * 8))
    const rec = source.reset().readUInt64Array()
    expect([...rec]).toStrictEqual(values)
  })

  it('Will write/read int64 arrays', () => {
    const values = [-(1n << 40n), ...bigRange(-(1n << 17n), 1n << 17n), 1n << 40n]

    expect(values.length).toBe(2 + (1 << 18))

    sink.reset().writeInt64Array(values)
    const alignment = 8
    expect(sink.drainTo(buffer)).toBe(alignment + (2 + (1 << 18)) * 8)
    const rec = source.reset().readInt64Array()
    expect([...rec]).toStrictEqual(values)
  })

  it('Will write/read typed int64 arrays', () => {
    const values = [-(1n << 40n), ...bigRange(-(1n << 17n), 1n << 17n), 1n << 40n]

    expect(values.length).toBe(2 + (1 << 18))

    const typed = new BigInt64Array(values)

    sink.reset().writeInt64Array(typed)
    const alignment = 8
    expect(sink.drainTo(buffer)).toBe(alignment + (2 + (1 << 18)) * 8)
    const rec = source.reset().readInt64Array()
    expect(rec).toStrictEqual(typed)
    expect([...rec]).toStrictEqual(values)
  })

  it('Will write/read int64 arrays by promotion', () => {
    const values = [-(1 << 40), ...range(-(1 << 17), 1 << 17), 1 << 40]

    expect(values.length).toBe(2 + (1 << 18))

    sink.reset().writeInt64Array(values)
    const alignment = 8
    expect(sink.drainTo(buffer)).toBe(alignment + (2 + (1 << 18)) * 8)
    const rec = source.reset().readInt64Array()
    expect([...rec].map(v => Number(v))).toStrictEqual(values)
  })

  it('Will write/read uint64 arrays by promotion', () => {
    const values = [...range(0, 1 << 18), 1 << 40]

    expect(values.length).toBe(1 + (1 << 18))

    sink.reset().writeUInt64Array(values)
    const alignment = 8
    expect(sink.drainTo(buffer)).toBe(alignment + (1 + (1 << 18)) * 8)
    const rec = source.reset().readUInt64Array()
    expect([...rec].map(v => Number(v))).toStrictEqual(values)
  })
})

describe('Floating Point Arrays', () => {
  const buffer = Buffer.alloc(4 * 1024 * 1024)
  const source = new Source(buffer)
  const sink = new Sink(16)

  it('Will write/read float32 arrays', () => {
    const values = [...range(0, 256)].map(v => v + 0.001)

    sink.reset().writeFloat32Array(values)
    expect(sink.drainTo(buffer)).toBe(4 + (256 * 4))
    const rec = source.reset().readFloat32Array()
    expect([...rec].map(v => v.toFixed(3))).toStrictEqual(values.map(v => v.toFixed(3)))
  })

  it('Will write/read float64 arrays', () => {
    const values = [...range(0, 256)].map(v => v + 0.00001)

    sink.reset().writeFloat64Array(values)
    const alignment = 8
    expect(sink.drainTo(buffer)).toBe(alignment + (256 * 8))
    const rec = source.reset().readFloat64Array()
    expect([...rec].map(v => v.toFixed(5))).toStrictEqual(values.map(v => v.toFixed(5)))
  })
})

describe('Data Compression', () => {
  const buffer = Buffer.alloc(128)
  const source = new Source(buffer)
  const sink = new Sink(16)

  it('Will pack boolean arrays', () => {
    // 1 8-byte stride
    const bools: boolean[] = [...range(0, 64)].map(i => (i & 1) === 0)
    expect(bools[0]).toBeTruthy()
    expect(bools[1]).toBeFalsy()

    sink.reset().writeBooleanArray(bools)
    expect(sink.drainTo(buffer)).toBe(4 + 8)
    let arr = source.reset().readBooleanArray()
    expect(arr).toStrictEqual(bools)

    const uneven = [...bools]
    // will require one extra stride
    uneven.push(false)

    sink.reset().writeBooleanArray(uneven)
    expect(sink.drainTo(buffer)).toBe(4 + 8 + 8)
    arr = source.reset().readBooleanArray()
    expect(arr).toStrictEqual(uneven)
  })
})

class VO {
  age: number

  constructor (age: number) {
    this.age = age
  }

  hash () {
    const a = this.age
    return a * a
  }
}

describe('Serialization', () => {
  const buffer = Buffer.alloc(512)
  const source = new Source(buffer)
  const sink = new Sink(16)

  let vo = new VO(4)

  it('Will serialize opaque', () => {
    expect(vo.hash()).toBe(vo.age * vo.age)

    const opaque = JSONSerializer<VO>()

    opaque.serialize(vo, sink)

    sink.drainTo(buffer)
    vo = opaque.deserialize(source.reset())

    expect(vo.age).toBe(4)
    expect(vo.hash).toBeUndefined()
  })

  it('Will serialize typed', () => {
    const typed = JSONSerializer<VO>(v => Object.setPrototypeOf(v, VO.prototype))

    typed.serialize(vo, sink.reset())
    sink.drainTo(buffer)

    vo = typed.deserialize(source.reset())
    expect(vo.age).toBe(4)
    expect(vo.hash).not.toBeUndefined()
    expect(vo.hash()).toBe(vo.age * vo.age)
  })
})

describe('Misc ops', () => {
  const source = new Source(Buffer.alloc(0))
  const sink = new Sink(16)

  it('Will unwrap', () => {
    sink.writeInt32(4)
    sink.writeInt64(999999999999n)

    let slice = sink.unwrap(false)

    expect(slice.byteLength).toBe(12)
    expect(slice.readInt32LE(0)).toBe(4)
    expect(slice.readBigInt64LE(4)).toBe(999999999999n)

    slice = sink.unwrap(true)

    expect(slice.byteLength).toBe(12)
    expect(slice.readInt32LE(0)).toBe(4)
    expect(slice.readBigInt64LE(4)).toBe(999999999999n)
  })

  it('Will replace', () => {
    sink.reset()
    sink.writeInt32(4)
    sink.writeInt64(999999999999n)

    const slice = sink.unwrap(false)

    source.replace(slice)
    expect(source.readInt32()).toBe(4)
    expect(source.readInt64()).toBe(999999999999n)

    source.replace(slice, undefined, undefined)
    expect(source.readInt32()).toBe(4)
    expect(source.readInt64()).toBe(999999999999n)

    source.replace(slice, 0, 4)
    expect(source.readInt32()).toBe(4)
    expect(() => source.readInt64()).toThrow(Error)

    source.replace(slice, undefined, 4)
    expect(source.readInt32()).toBe(4)
    expect(() => source.readInt64()).toThrow(Error)

    source.replace(slice, 4)
    expect(source.readInt64()).toBe(999999999999n)
    expect(() => source.readInt8()).toThrow(Error)

    source.replace(slice, 4, undefined)
    expect(source.readInt64()).toBe(999999999999n)
    expect(() => source.readInt8()).toThrow(Error)
  })

  it('Will shrink', () => {
    sink.reset()
    sink.shrink()
    expect(sink.drainTo(Buffer.alloc(10))).toBe(0)
  })

  it('Will Fail on bad offsets', () => {
    expect(() => sink.drainTo(Buffer.alloc(0), -1)).toThrow(Error)

    expect(() => sink.drainTo(Buffer.alloc(0), 0, -1)).toThrow(Error)
  })

  it('Will Fail on out of range access', () => {
    expect(() => sink.drainTo(Buffer.alloc(0), 0, 1024)).toThrow(Error)
  })
})

describe('Absolute reads', () => {
  const buffer = Buffer.alloc(128)
  const source = new Source(buffer)
  const sink = new Sink(16)
  const now = new Date()
  const utf = '中华人民共和国'

  it('Will read with absolute positions', () => {
    sink.writeInt8(-8)
    sink.writeUInt8(8)
    sink.writeInt16(-7000)
    sink.writeUInt16(7000)
    sink.writeInt32(-999999)
    sink.writeUInt32(999999)
    sink.writeInt64(BigInt(Number.MIN_SAFE_INTEGER) - 1n)
    sink.writeUInt64(BigInt(Number.MAX_SAFE_INTEGER) + 1n)
    sink.writeFloat32(3.1415)
    sink.writeFloat64(2.71828)
    sink.writeDate(now)
    sink.writeUTF(utf)

    sink.drainTo(buffer)

    let pos = 0
    expect(source.getInt8(pos)).toBe(-8)
    pos += 1
    expect(source.getUInt8(pos)).toBe(8)
    pos += 1
    expect(source.getInt16(pos)).toBe(-7000)
    pos += 2
    expect(source.getUInt16(pos)).toBe(7000)
    pos += 2
    expect(source.getInt32(pos)).toBe(-999999)
    pos += 4
    expect(source.getUInt32(pos)).toBe(999999)
    pos += 4
    expect(source.getInt64(pos)).toBe(BigInt(Number.MIN_SAFE_INTEGER) - 1n)
    pos += 8
    expect(source.getUInt64(pos)).toBe(BigInt(Number.MAX_SAFE_INTEGER) + 1n)
    pos += 8
    expect(source.getFloat32(pos).toFixed(4)).toBe(3.1415.toFixed(4))
    pos += 4
    expect(source.getFloat64(pos).toFixed(5)).toBe(2.71828.toFixed(5))
    pos += 8
    expect(source.getDate(pos)).toStrictEqual(now)
    pos += 8
    expect(source.getUTF(pos)).toStrictEqual(utf)
  })
})

describe('Relative reads', () => {
  const buffer = Buffer.alloc(128)
  const source = new Source(buffer)
  const sink = new Sink(16)
  const now = new Date()
  const utf = '中华人民共和国'

  it('Will read with advancing position', () => {
    sink.writeInt8(-8)
    sink.writeUInt8(8)
    sink.writeInt16(-7000)
    sink.writeUInt16(7000)
    sink.writeInt32(-999999)
    sink.writeUInt32(999999)
    sink.writeInt64(BigInt(Number.MIN_SAFE_INTEGER) - 1n)
    sink.writeUInt64(BigInt(Number.MAX_SAFE_INTEGER) + 1n)
    sink.writeFloat32(3.1415)
    sink.writeFloat64(2.71828)
    sink.writeDate(now)
    sink.writeUTF(utf)

    sink.drainTo(buffer)

    expect(source.readInt8()).toBe(-8)
    expect(source.readUInt8()).toBe(8)
    expect(source.readInt16()).toBe(-7000)
    expect(source.readUInt16()).toBe(7000)
    expect(source.readInt32()).toBe(-999999)
    expect(source.readUInt32()).toBe(999999)
    expect(source.readInt64()).toBe(BigInt(Number.MIN_SAFE_INTEGER) - 1n)
    expect(source.readUInt64()).toBe(BigInt(Number.MAX_SAFE_INTEGER) + 1n)
    expect(source.readFloat32().toFixed(4)).toBe(3.1415.toFixed(4))
    expect(source.readFloat64().toFixed(5)).toBe(2.71828.toFixed(5))
    expect(source.readDate()).toStrictEqual(now)
    expect(source.readUTF()).toStrictEqual(utf)
  })
})
