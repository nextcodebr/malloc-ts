import { MIN_SIGNED_32, MAX_SIGNED_32 } from '../32bit.math'
import { PathLike, readFileSync, existsSync, mkdirSync, writeFileSync } from 'fs'
import { dirname } from 'path'
import { URL } from 'url'

const SOFT_MAX_ARRAY_LENGTH = MAX_SIGNED_32 - 8

const EMPTY = Buffer.allocUnsafe(0)

const hugeLength = (oldLength: number, minGrowth: number) => {
  const minLength = oldLength + minGrowth
  if (minLength < 0) { // overflow
    throw new Error(
      `Required array length ${oldLength}  + ${minGrowth}  is too large`)
  } else if (minLength <= SOFT_MAX_ARRAY_LENGTH) {
    return SOFT_MAX_ARRAY_LENGTH
  } else {
    return minLength
  }
}

export const newLength = (oldLength: number, minGrowth: number, prefGrowth: number) => {
  const prefLength = oldLength + Math.max(minGrowth, prefGrowth)

  if (prefLength > 0 && prefLength <= SOFT_MAX_ARRAY_LENGTH) {
    return prefLength
  } else {
    return hugeLength(oldLength, minGrowth)
  }
}

const eof = (request: number, avail: number) => new Error(`Requested ${request} but there's only ${avail} remaining`)

const checkEof = (request: number, avail: number) => {
  if (request > avail) {
    throw eof(request, avail)
  }
}

const positive = (n: number) => {
  if (n < 0) {
    throw new Error(`${n}<0`)
  }
  return n
}

const pick = (a: number, b?: number) => {
  const n = (b ?? a)
  return positive(n)
}

const pack = (a: boolean[], stride: number) => {
  const start = stride * 64
  const end = Math.min(a.length, (stride + 1) * 64)
  let shift = 0n
  let rv = 0n
  for (let i = start; i < end; i++) {
    if (a[i]) {
      rv |= (1n << shift)
    }
    shift++
  }
  return rv
}

const unpack = (b: bigint, bits: number, rv: boolean[]) => {
  const end = BigInt(bits)

  for (let i = 0n; i < end; i++) {
    rv.push((b & (1n << i)) !== 0n)
  }
}

const align = (n: number, alignment: number) => {
  return (n + alignment - 1) & -alignment
}

export const sync = (src: Buffer, dst: string) => {
  if (!existsSync(dirname(dst))) {
    mkdirSync(dirname(dst), { recursive: true })
  }
  writeFileSync(dst, src)
}

export const isBuffer = (o?: any): boolean => {
  return o?.constructor?.name === 'Buffer'
}

export const inflate = (source: PathLike): Buffer => {
  if (isBuffer(source)) {
    return source as Buffer
  }

  let path: string

  if (typeof source === 'string') {
    path = source
  } else {
    const url = source as URL
    if (url.protocol === 'file:') {
      path = url.pathname
    } else {
      throw new Error(`Unsupported protocol: ${url.protocol}`)
    }
  }

  return readFileSync(path)
}

export const copyOf = (src: Buffer, start?: number, end?: number) => {
  start = start ?? 0
  end = end ?? src.byteLength
  if (end < start) {
    throw new Error(`${end}<${start}`)
  }
  const tmp = Buffer.allocUnsafe(end - start)
  src.copy(tmp, 0, start, end)
  return tmp
}

export class Source {
  protected buffer: Buffer = EMPTY
  protected pos: number = 0
  protected mark?: number

  constructor (buffer: Buffer, offset?: number, length?: number) {
    this.replace(buffer, offset, length)
  }

  public reset () {
    if (this.mark) {
      this.pos = this.mark
    } else {
      this.pos = 0
    }
    this.mark = undefined
    return this
  }

  public replace (buffer: Buffer, offset?: number, length?: number) {
    offset = offset ?? 0
    length = length ?? buffer.byteLength
    this.buffer = buffer.slice(offset, offset + length)
    this.pos = 0
    this.mark = undefined

    return this
  }

  get length () {
    return this.buffer.byteLength
  }

  get available () {
    return this.length - this.pos
  }

  private require (n: number, off?: number) {
    checkEof(n, this.length - pick(this.pos, off))
  }

  readBoolean (): boolean {
    this.require(1)
    let p = this.pos
    const rv = this.buffer.readUInt8(p++)
    this.pos = p
    return rv === 1
  }

  readBooleanArray (): boolean[] {
    let { pos, buffer } = this
    this.require(4, pos)

    const length = buffer.readUInt32LE(pos)
    pos += 4
    const strides = length >>> 6
    const tail = length & 63

    const rv: boolean[] = []

    for (let i = 0; i < strides; i++) {
      unpack(buffer.readBigUInt64LE(pos), 64, rv)
      pos += 8
    }

    if (tail) {
      unpack(buffer.readBigUInt64LE(pos), tail, rv)
      pos += 8
    }

    this.pos = pos

    return rv
  }

  getInt8 (off: number) {
    this.require(1, off)
    return this.buffer.readInt8(off)
  }

  getUInt8 (off: number) {
    this.require(1, off)
    return this.buffer.readUInt8(off)
  }

  getInt16 (off: number) {
    this.require(2, off)
    return this.buffer.readInt16LE(off)
  }

  getUInt16 (off: number) {
    this.require(2, off)
    return this.buffer.readUInt16LE(off)
  }

  getInt32 (off: number) {
    this.require(4, off)
    return this.buffer.readInt32LE(off)
  }

  getUInt32 (off: number) {
    this.require(4, off)
    return this.buffer.readUInt32LE(off)
  }

  getInt64 (off: number) {
    this.require(8, off)
    return this.buffer.readBigInt64LE(off)
  }

  getUInt64 (off: number) {
    this.require(8, off)
    return this.buffer.readBigUInt64LE(off)
  }

  getFloat32 (off: number) {
    this.require(4, off)
    return this.buffer.readFloatLE(off)
  }

  getFloat64 (off: number) {
    this.require(8, off)
    return this.buffer.readDoubleLE(off)
  }

  getDate (off: number) {
    return new Date(Number(this.getInt64(off)))
  }

  getUTF (off: number) {
    this.require(4, off)
    const len = this.buffer.readUInt32LE(off)
    this.require(4 + len, off)
    const rv = this.buffer.slice(off + 4, off + 4 + len).toString('utf8')

    return rv
  }

  readInt8 (): number {
    this.require(1)
    let p = this.pos
    const rv = this.buffer.readInt8(p++)
    this.pos = p
    return rv
  }

  readInt16 (): number {
    this.require(2)
    const rv = this.buffer.readInt16LE(this.pos)
    this.pos += 2
    return rv
  }

  readInt32 (): number {
    this.require(4)
    const rv = this.buffer.readInt32LE(this.pos)
    this.pos += 4

    return rv
  }

  readFloat32 (): number {
    this.require(4)
    const rv = this.buffer.readFloatLE(this.pos)
    this.pos += 4

    return rv
  }

  readInt64 (): bigint {
    this.require(8)
    const rv = this.buffer.readBigInt64LE(this.pos)
    this.pos += 8

    return rv
  }

  readFloat64 (): number {
    this.require(8)
    const rv = this.buffer.readDoubleLE(this.pos)
    this.pos += 8

    return rv
  }

  readUInt8 (): number {
    this.require(1)
    let p = this.pos
    const rv = this.buffer.readUInt8(p++)
    this.pos = p
    return rv
  }

  readUInt16 (): number {
    this.require(2)
    const rv = this.buffer.readUInt16LE(this.pos)
    this.pos += 2
    return rv
  }

  readUInt32 (): number {
    this.require(4)
    const rv = this.buffer.readUInt32LE(this.pos)
    this.pos += 4

    return rv
  }

  readUInt64 (): bigint {
    this.require(8)
    const rv = this.buffer.readBigInt64LE(this.pos)
    this.pos += 8

    return rv
  }

  readUTF (): string {
    let p = this.pos
    this.require(4)
    const len = this.buffer.readUInt32LE(p)
    p += 4
    this.require(4 + len)
    const rv = this.buffer.slice(p, p + len).toString('utf8')
    this.pos = p + len

    return rv
  }

  readDate (): Date {
    const value = Number(this.readInt64())

    return new Date(value)
  }

  private sliceForCopy (scale: number) {
    this.require(4)
    let p = this.pos
    const buffer = this.buffer
    const length = (buffer.readUInt32LE(p) << scale)
    p += 4
    const pa = align(p, 1 << scale)
    this.require(4 + length + (pa - p))
    this.pos += pa + length

    return buffer.slice(pa, pa + length)
  }

  private newArray<T> (Type: new (src: ArrayBufferLike, offset: number, scale: number) => T, scale: number) {
    const slice = this.sliceForCopy(scale)
    return new Type(slice.buffer, slice.byteOffset, slice.byteLength >>> scale)
  }

  readInt8Array (): Int8Array {
    return Int8Array.from(this.sliceForCopy(0))
  }

  readUInt8Array (): Uint8Array {
    return Uint8Array.from(this.sliceForCopy(0))
  }

  readInt16Array (): Int16Array {
    return this.newArray(Int16Array, 1)
  }

  readUInt16Array (): Uint16Array {
    return this.newArray(Uint16Array, 1)
  }

  readInt32Array (): Int32Array {
    return this.newArray(Int32Array, 2)
  }

  readUInt32Array (): Uint32Array {
    return this.newArray(Uint32Array, 2)
  }

  readInt64Array (): BigInt64Array {
    return this.newArray(BigInt64Array, 3)
  }

  readUInt64Array (): BigUint64Array {
    return this.newArray(BigUint64Array, 3)
  }

  readFloat32Array (): Float32Array {
    return this.newArray(Float32Array, 2)
  }

  readFloat64Array (): Float64Array {
    return this.newArray(Float64Array, 3)
  }

  readStream<T> (deserialize: (src: Source) => T): T[] {
    const len = this.readUInt32()
    const rv: T[] = []

    for (let i = 0; i < len; i++) {
      rv.push(deserialize(this))
    }

    return rv
  }

  readUFloat16 () {
    const v = this.readUInt16()
    const d = v >>> 8
    const f = v & 0xFF

    return d + (f / 100)
  }

  readVInt32 () {
    this.require(1)
    const u = this.buffer
    let p = this.pos
    let b = u.readInt8(p++)
    let v = b & 0x7F

    for (let s = 7; (b & 0x80) !== 0 && s <= 28; s += 7) {
      this.require(1)
      v |= ((b = u.readInt8(p++)) & 0x7F) << s
    }

    this.pos = p

    return v
  }

  private slab (from?: number, length?: number, copy = false, advance = true) {
    from = from !== undefined && from >= 0 && from < this.length ? from : this.pos
    length = length !== undefined && length >= 0 && (length - from) <= this.length ? length : this.length - from

    if (length <= 0) {
      return EMPTY
    }

    let rv = this.buffer.slice(from, from + length)
    if (copy) {
      const tmp = Buffer.allocUnsafe(length)
      rv.copy(tmp)
      rv = tmp
    }

    if (advance) {
      this.pos += length
    }

    return rv
  }

  slice (length: number, copy = false) {
    return this.slab(undefined, length, copy, true)
  }

  markPos () {
    this.mark = this.pos
  }
}

export class Sink {
  protected buffer: Buffer
  protected pos: number

  constructor (cap: number) {
    this.buffer = Buffer.allocUnsafe(cap)
    this.pos = 0
  }

  require (n: number) {
    const rem = this.remaining
    const minGrowth = n - rem
    if (minGrowth > 0) {
      const cap = this.capacity
      const nextLen = newLength(cap, minGrowth, cap)
      const next = Buffer.allocUnsafe(nextLen)
      this.buffer.copy(next)
      this.buffer = next
    }
  }

  reset () {
    this.pos = 0
    return this
  }

  get position () {
    return this.pos
  }

  get remaining () {
    return this.capacity - this.pos
  }

  get capacity () {
    return this.buffer.byteLength
  }

  public replace (buffer: Buffer, offset?: number, length?: number) {
    offset = offset ?? 0
    length = length ?? buffer.byteLength
    this.buffer = buffer.slice(offset, offset + length)
    this.pos = 0

    return this
  }

  drainTo (dst: Buffer, off?: number, length?: number) {
    off = off ?? 0
    length = length ?? dst.byteLength - off

    if (off < 0 || length < 0 || (off + length) > dst.byteLength) {
      throw new Error(`Out of range access (start=${off}, end=${off + length},valid=[0,${dst.byteLength}) )`)
    }

    const end = Math.min(length, this.pos)

    return this.buffer.copy(dst, off, 0, end)
  }

  shrink () {
    if (this.buffer.byteLength > this.pos) {
      const next = Buffer.allocUnsafe(this.pos)
      this.buffer.copy(next, 0, 0, this.pos)
      this.buffer = next
    }
  }

  private memcpy<T extends number | bigint> (a: T[] | RelativeIndexable<T> & { length: number }, scale: number, store: (v: T, pos: number) => number, off?: number, big = false) {
    let pos = pick(this.pos, off)
    const length = (a.length << scale)
    if (off === undefined) {
      this.require(16 + length)
    }
    const buffer = this.buffer
    buffer.writeUInt32LE(a.length, pos)
    pos += 4
    pos = align(pos, 1 << scale)

    const src: Buffer | undefined = (a as any).buffer
    if (src) {
      const tmp = Buffer.from(src)
      tmp.copy(buffer, pos, 0, length)
      pos += length
    } else {
      const inc = 1 << scale
      if (big) {
        const n = a as Array<bigint>
        const s = (store as (v: bigint, pos: number) => number).bind(buffer)
        s.bind(buffer)
        for (const v of n) {
          s(v, pos)
          pos += inc
        }
      } else {
        const n = a as number[]
        const s = (store as (v: number, pos: number) => number).bind(buffer)
        for (const v of n) {
          s(v, pos)
          pos += inc
        }
      }
    }
    if (off === undefined) {
      this.pos = pos
    }
  }

  writeBoolean (v: boolean) {
    this.require(1)
    this.buffer.writeUInt8(v ? 1 : 0, this.pos++)
  }

  writeBooleanArray (a: boolean[], off?: number) {
    let pos = pick(this.pos, off)
    const strides = (a.length >>> 6) + ((a.length & 63) === 0 ? 0 : 1)

    if (off === undefined) {
      this.require(strides * 8 + 4)
    }
    const buffer = this.buffer
    buffer.writeUInt32LE(a.length, pos)
    pos += 4

    for (let i = 0; i < strides; i++) {
      const n = pack(a, i)
      buffer.writeBigUInt64LE(n, pos)
      pos += 8
    }

    if (off === undefined) {
      this.pos = pos
    }
  }

  writeInt8 (v: number) {
    this.require(1)
    this.buffer.writeInt8(v, this.pos++)
  }

  writeInt8Array (a: number[] | Int8Array, off?: number) {
    this.memcpy(a, 0, this.buffer.writeInt8, off)
  }

  writeInt16 (v: number) {
    this.require(1)
    this.buffer.writeInt16LE(v, this.pos)
    this.pos += 2
  }

  writeInt16Array (a: number[] | Int16Array, off?: number) {
    this.memcpy(a, 1, this.buffer.writeInt16LE, off)
  }

  writeInt32 (v: number) {
    this.require(4)
    this.buffer.writeInt32LE(v, this.pos)
    this.pos += 4
  }

  writeInt32Array (a: number[] | Int32Array, off?: number) {
    this.memcpy(a, 2, this.buffer.writeInt32LE, off)
  }

  writeInt64 (v: bigint) {
    this.require(8)
    this.buffer.writeBigInt64LE(v, this.pos)
    this.pos += 8
  }

  writeInt64Array (a: number[] | Array<bigint> | BigInt64Array, off?: number) {
    const big = typeof a[0] === 'bigint'

    if (!big) {
      const store = (v: number, pos: number) => this.buffer.writeBigInt64LE(BigInt(v), pos)
      this.memcpy(a as number[], 3, store, off)
    } else {
      this.memcpy(a as Array<bigint> | BigInt64Array, 3, this.buffer.writeBigInt64LE, off, big)
    }
  }

  writeFloat32 (v: number) {
    this.require(4)
    this.buffer.writeFloatLE(v, this.pos)
    this.pos += 4
  }

  writeFloat32Array (a: number[] | Float32Array, off?: number) {
    this.memcpy(a, 2, this.buffer.writeFloatLE, off)
  }

  writeFloat64 (v: number) {
    this.require(8)
    this.buffer.writeDoubleLE(v, this.pos)
    this.pos += 8
  }

  writeFloat64Array (a: number[] | Float64Array, off?: number) {
    this.memcpy(a, 3, this.buffer.writeDoubleLE, off)
  }

  writeUInt8 (v: number) {
    this.require(1)
    this.buffer.writeUInt8(v, this.pos++)
  }

  writeUInt8Array (a: number[] | Uint8Array, off?: number) {
    this.memcpy(a, 0, this.buffer.writeUInt8, off)
  }

  writeUInt16 (v: number) {
    this.require(2)
    this.buffer.writeUInt16LE(v, this.pos)
    this.pos += 2
  }

  writeUInt16Array (a: number[] | Uint16Array, off?: number) {
    this.memcpy(a, 1, this.buffer.writeUInt16LE, off)
  }

  writeUInt32 (v: number) {
    this.require(4)
    this.buffer.writeUInt32LE(v, this.pos)
    this.pos += 4
  }

  writeUInt32Array (a: number[] | Uint32Array, off?: number) {
    this.memcpy(a, 2, this.buffer.writeUInt32LE, off)
  }

  writeUInt64 (v: bigint) {
    this.require(8)
    this.buffer.writeBigUInt64LE(v, this.pos)
    this.pos += 8
  }

  writeUInt64Array (a: number[] | Array<bigint> | BigInt64Array, off?: number) {
    const big = typeof a[0] === 'bigint'

    if (!big) {
      const store = (v: number, pos: number) => this.buffer.writeBigUInt64LE(BigInt(v), pos)
      this.memcpy(a as number[], 3, store, off)
    } else {
      this.memcpy(a as Array<bigint> | BigInt64Array, 3, this.buffer.writeBigUInt64LE, off, big)
    }
  }

  writeDate (d: Date) {
    this.writeInt64(BigInt(d.getTime()))
  }

  writeUTF (s: string) {
    const len = Buffer.byteLength(s, 'utf8')
    this.require(len + 4)
    let off = this.pos
    this.buffer.writeUInt32LE(len, off)
    this.buffer.write(s, off += 4, 'utf8')
    this.pos = off + len
  }

  writeStream<T> (a: T[] | Generator<T, void, unknown>, serialize: (v: T, dst: Sink) => void) {
    const mark = this.pos
    this.pos += 4
    let n = 0
    for (const v of a) {
      serialize(v, this)
      n++
    }
    this.require(4)
    this.buffer.writeUInt32LE(n, mark)
  }

  write (src: Buffer, off?: number, len?: number): number {
    off = off ?? 0
    len = len ?? src.byteLength
    this.require(this.pos + len)
    src.copy(this.buffer, this.pos, off, off + len)
    this.pos += len

    return len
  }

  writeUFloat16 (v: number) {
    const d = ~~v
    if (d < 0 || d > 255) {
      throw new Error(`Integer part (${d}) > 8 bytes`)
    }
    const f = ((v - d) * 100)
    let f0 = ~~f
    const f1 = ((f - f0) * 100)
    if (f1 > 50) {
      f0++
    }

    this.require(2)
    this.buffer.writeUInt16LE((d << 8) | f0)
    this.pos += 2
  }

  writeVInt32 (v: number) {
    if (v < MIN_SIGNED_32 || v > MAX_SIGNED_32) {
      throw new Error(`${v} is out of range [${MIN_SIGNED_32},${MAX_SIGNED_32})`)
    }
    this.require(5)
    const b = this.buffer
    let p = this.pos

    if ((v >>> 7) === 0) {
      b.writeUInt8(v, p++)
    } else if ((v >>> 14) === 0) {
      b.writeUInt8((v & 0x7F) | 0x80, p++)
      b.writeUInt8((v >>> 7), p++)
    } else if (v >>> 21 === 0) {
      b.writeUInt8((v & 0x7F | 0x80), p++)
      b.writeUInt8((v >>> 7 | 0x80) & 0xFF, p++)
      b.writeUInt8((v >>> 14), p++)
    } else if (v >>> 28 === 0) {
      b.writeUInt8((v & 0x7F | 0x80), p++)
      b.writeUInt8((v >>> 7 | 0x80) & 0xFF, p++)
      b.writeUInt8((v >>> 14 | 0x80) & 0xFF, p++)
      b.writeUInt8((v >>> 21), p++)
    } else {
      b.writeUInt8((v & 0x7F | 0x80), p++)
      b.writeUInt8((v >>> 7 | 0x80) & 0xFF, p++)
      b.writeUInt8((v >>> 14 | 0x80) & 0xFF, p++)
      b.writeUInt8((v >>> 21 | 0x80) & 0xFF, p++)
      b.writeUInt8((v >>> 28), p++)
    }

    this.pos = p
  }

  putInt32 (off: number, value: number) {
    this.buffer.writeInt32LE(value, off)
  }

  skip (n: number) {
    this.require(n)
    this.pos += n
    return this
  }

  unwrap (copy = false) {
    let rv = this.buffer.slice(0, this.pos)
    if (copy) {
      const tmp = Buffer.allocUnsafe(rv.byteLength)
      rv.copy(tmp)
      rv = tmp
    }
    return rv
  }
}

export interface Serializer<T> {
  deserialize: (src: Source) => T

  serialize: (v: T, dst: Sink) => void
}

export const JSONSerializer = <T> (reviver?: (opaque: any) => T): Serializer<T> => {
  return {
    deserialize: (src: Source) => {
      const len = src.readUInt32()

      const o = JSON.parse((src.slice(len) as unknown) as string)

      return reviver ? reviver(o) : o as T
    },
    serialize: (v: T, dst: Sink) => {
      const chunk = Buffer.from(JSON.stringify(v))
      dst.writeUInt32(chunk.byteLength)
      dst.write(chunk)
    }
  }
}

const SharedSink = new Sink(16)
const SharedSource = new Source(EMPTY)

// Behold the beauty of a single threaded runtime
export const Pipe = {
  sink: SharedSink,
  source: SharedSource,
  shared: {
    sink: (buffer: Buffer) => SharedSink.replace(buffer),
    source: (buffer: Buffer) => SharedSource.replace(buffer)
  }
}
