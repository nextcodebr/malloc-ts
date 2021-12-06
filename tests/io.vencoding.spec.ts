import { Sink, Source } from '@/io'
import { MIN_SIGNED_32, MAX_SIGNED_32 } from '@/32bit.math'

describe('Variable Length Int32 will', () => {
  const buff = Buffer.alloc(256)
  const source = new Source(buff)
  const sink = new Sink(16)

  it('encode numbers in [0,128) with 1 byte', () => {
    let s = 0
    while (s >>> 7 === 0) {
      sink.reset().writeVInt32(s)
      expect(sink.drainTo(buff)).toBe(1)
      const r = source.reset().readVInt32()
      expect(r).toBe(s)
      s++
    }
  })

  it('encode numbers in [128,16384) with 2 bytes', () => {
    let s = 1 << 7
    while (s >>> 14 === 0) {
      sink.reset().writeVInt32(s)
      expect(sink.drainTo(buff)).toBe(2)
      const r = source.reset().readVInt32()
      expect(r).toBe(s)
      s++
    }
  })

  it('encode numbers in [16384,2097152) with 3 bytes', () => {
    let min = 1 << 14
    const overflow = 1 << 21
    const max = overflow - 1
    while (min >>> 21 === 0) {
      sink.reset().writeVInt32(min)
      expect(sink.drainTo(buff)).toBe(3)
      const r = source.reset().readVInt32()
      expect(r).toBe(min)
      min = 1 + (min + overflow) >>> 1
    }

    sink.reset().writeVInt32(max)
    expect(sink.drainTo(buff)).toBe(3)
    const r = source.reset().readVInt32()
    expect(r).toBe(max)
  })

  it(`encode numbers in [2097152,${1 << 28}) with 4 bytes`, () => {
    let min = 1 << 21
    const overflow = 1 << 28
    const max = overflow - 1
    while (min >>> 28 === 0) {
      sink.reset().writeVInt32(min)
      expect(sink.drainTo(buff)).toBe(4)
      const r = source.reset().readVInt32()
      expect(r).toBe(min)
      min = 1 + (min + overflow) >>> 1
    }

    sink.reset().writeVInt32(max)
    expect(sink.drainTo(buff)).toBe(4)
    const r = source.reset().readVInt32()
    expect(r).toBe(max)
  })

  it(`encode numbers in [${1 << 28}, ${MAX_SIGNED_32}] with 5 bytes`, () => {
    let min = 1 << 28
    const max = MAX_SIGNED_32
    while (min < max) {
      sink.reset().writeVInt32(min)
      expect(sink.drainTo(buff)).toBe(5)
      const r = source.reset().readVInt32()
      expect(r).toBe(min)
      min = (min + max) >>> 1
      min++
    }

    sink.reset().writeVInt32(max)
    expect(sink.drainTo(buff)).toBe(5)
    const r = source.reset().readVInt32()
    expect(r).toBe(max)
  })

  it(`encode negative numbers in [${MIN_SIGNED_32}, 0) with 5 bytes`, () => {
    let min = MIN_SIGNED_32
    const max = -1

    while (min < max) {
      sink.reset().writeVInt32(min)
      expect(sink.drainTo(buff)).toBe(5)
      const r = source.reset().readVInt32()
      expect(r).toBe(min)
      min = ((min + max) / 2) >> 0
    }

    sink.reset().writeVInt32(max)
    expect(sink.drainTo(buff)).toBe(5)
    const r = source.reset().readVInt32()
    expect(r).toBe(max)
  })

  it('throw error on invalid ranges', () => {
    expect(() => sink.reset().writeVInt32(MAX_SIGNED_32 + 1)).toThrow(Error)

    expect(() => sink.reset().writeVInt32(MIN_SIGNED_32 - 1)).toThrow(Error)
  })
})

describe('Variable Length UFloat16 will', () => {
  const buff = Buffer.alloc(256)
  const source = new Source(buff)
  const sink = new Sink(16)

  it('encode small floats with 2 bytes (round up)', () => {
    const f = 12.6451

    expect(f.toFixed(2)).toBe('12.65')

    sink.reset().writeUFloat16(f)
    expect(sink.drainTo(buff)).toBe(2)
    const r = source.reset().readUFloat16()

    expect(r.toFixed(2)).toBe(f.toFixed(2))
  })

  it('encode small floats with 2 bytes (round down - edge)', () => {
    const f = 12.6450

    expect(f.toFixed(2)).toBe('12.64')

    sink.reset().writeUFloat16(f)
    expect(sink.drainTo(buff)).toBe(2)
    const r = source.reset().readUFloat16()

    expect(r.toFixed(2)).toBe(f.toFixed(2))
  })

  it('encode small floats with 2 bytes (round down - lower)', () => {
    const f = 12.6449

    expect(f.toFixed(2)).toBe('12.64')

    sink.reset().writeUFloat16(f)
    expect(sink.drainTo(buff)).toBe(2)
    const r = source.reset().readUFloat16()

    expect(r.toFixed(2)).toBe(f.toFixed(2))
  })

  it('fail to encode floats larger than 255', () => {
    const f = 256.6449

    expect(() => sink.reset().writeUFloat16(f)).toThrow(Error)
  })
})
