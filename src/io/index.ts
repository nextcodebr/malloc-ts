import { PathLike, readFileSync, existsSync, mkdirSync, writeFileSync } from 'fs'
import { dirname } from 'path'
import { URL } from 'url'

const SOFT_MAX_ARRAY_LENGTH = 0x7FFFFFFF - 8

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
    checkEof(n, this.length - (off ?? this.pos))
  }

  readOrEof () {
    return (this.pos < this.length) ? this.buffer.readUInt8(this.pos++) : -1
  }

  readBoolean (): boolean {
    this.require(1)
    let p = this.pos
    const rv = this.buffer.readUInt8(p++)
    this.pos = p
    return rv === 1
  }

  readBooleanArray (off?: number): boolean[] {
    let { pos, buffer } = this
    pos = pick(pos, off)
    this.require(4, pos)

    const length = buffer.readUInt32LE(pos)
    pos += 4
    const strides = Math.floor(length / 64)
    const tail = length % 64

    const rv: boolean[] = []

    for (let i = 0; i < strides; i++) {
      unpack(buffer.readBigUInt64LE(pos), 64, rv)
      pos += 8
    }

    if (tail) {
      unpack(buffer.readBigUInt64LE(pos), tail, rv)
      pos += 8
    }

    if (off !== undefined) {
      this.pos = pos
    }

    return rv
  }

  getInt8 (off: number) {
    this.require(1, off)
    return this.buffer.readInt8(off)
  }

  getInt16 (off: number) {
    this.require(2, off)
    return this.buffer.readInt16LE(off)
  }

  getInt32 (off: number) {
    this.require(4, off)
    return this.buffer.readInt32LE(off)
  }

  getInt64 (off: number) {
    this.require(8, off)
    return this.buffer.readBigInt64LE(off)
  }

  getFloat32 (off: number) {
    this.require(4, off)
    return this.buffer.readFloatLE(off)
  }

  getFloat64 (off: number) {
    this.require(8, off)
    return this.buffer.readDoubleLE(off)
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
    let p = this.pos
    const rv = this.buffer.readInt16LE(p++)
    this.pos = p
    return rv
  }

  readInt32 (): number {
    this.require(4)
    const rv = this.buffer.readInt32LE(this.pos)
    this.pos += 4

    return rv
  }

  readInt64 (): bigint {
    this.require(8)
    const rv = this.buffer.readBigInt64LE(this.pos)
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
    this.require(1)
    let p = this.pos
    const rv = this.buffer.readUInt16LE(p++)
    this.pos = p
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
    const value = Number(this.readUInt64())

    return new Date(value)
  }

  readStream<T> (deserialize: (src: Source) => T): T[] {
    const len = this.readUInt32()
    const rv: T[] = []

    for (let i = 0; i < len; i++) {
      rv.push(deserialize(this))
    }

    return rv
  }

  read (dst: Buffer, off?: number, len?: number): number {
    const avail = this.available

    if (avail <= 0) {
      return -1
    }

    off = off ?? 0
    len = len ?? dst.byteLength - off

    if (len > avail) {
      len = avail
    }

    if (len <= 0) {
      return 0
    }

    this.buffer.copy(dst, off, this.pos, this.pos + len)
    this.pos += len

    return len
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
    let { pos, buffer } = this
    pos = pick(pos, off)
    if (off === undefined) {
      this.require(scale * a.length + 4)
    }
    buffer.writeUInt32LE(a.length, pos)
    pos += 4

    const src: Buffer | undefined = (a as any).buffer
    if (src) {
      const tmp = Buffer.from(buffer)
      tmp.copy(buffer, pos, 0, a.length * scale)
      pos += a.length * scale
    } else {
      if (big) {
        const n = a as Array<bigint>
        const s = store as (v: bigint, pos: number) => number
        for (const v of n) {
          s(v, pos)
          pos += scale
        }
      } else {
        const n = a as number[]
        const s = store as (v: number, pos: number) => number
        for (const v of n) {
          s(v, pos)
          pos += scale
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
    let { pos, buffer } = this
    pos = pick(pos, off)

    const strides = Math.floor(a.length / 64) + (a.length % 64 ? 1 : 0)

    if (off === undefined) {
      this.require(strides * 8 + 4)
    }
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
    this.memcpy(a, 1, this.buffer.writeInt8, off)
  }

  writeInt16 (v: number) {
    this.require(1)
    this.buffer.writeInt16LE(v, this.pos)
    this.pos += 2
  }

  writeInt16Array (a: number[] | Int16Array, off?: number) {
    this.memcpy(a, 2, this.buffer.writeInt16LE, off)
  }

  writeInt32 (v: number) {
    this.require(4)
    this.buffer.writeInt32LE(v, this.pos)
    this.pos += 4
  }

  writeInt32Array (a: number[] | Int32Array, off?: number) {
    this.memcpy(a, 4, this.buffer.writeInt32LE, off)
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
      this.memcpy(a as number[], 8, store, off)
    } else {
      this.memcpy(a as Array<bigint> | BigInt64Array, 8, this.buffer.writeBigInt64LE, off, big)
    }
  }

  writeFloat32 (v: number) {
    this.require(4)
    this.buffer.writeFloatLE(v, this.pos)
    this.pos += 4
  }

  writeFloat32Array (a: number[] | Float32Array, off?: number) {
    this.memcpy(a, 4, this.buffer.writeFloatLE, off)
  }

  writeFloat64 (v: number) {
    this.require(8)
    this.buffer.writeDoubleLE(v, this.pos)
    this.pos += 8
  }

  writeFloat64Array (a: number[] | Float64Array, off?: number) {
    this.memcpy(a, 8, this.buffer.writeDoubleLE, off)
  }

  writeUInt8 (v: number) {
    this.require(1)
    this.buffer.writeUInt8(v, this.pos++)
  }

  writeUInt8Array (a: number[] | Uint8Array, off?: number) {
    this.memcpy(a, 1, this.buffer.writeUInt8, off)
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
    this.memcpy(a, 1, this.buffer.writeUInt32LE, off)
  }

  writeUInt64 (v: bigint) {
    this.require(8)
    this.buffer.writeBigUInt64LE(v, this.pos)
    this.pos += 8
  }

  writeUint64Array (a: number[] | Array<bigint> | BigInt64Array, off?: number) {
    const big = typeof a[0] === 'bigint'

    if (!big) {
      const store = (v: number, pos: number) => this.buffer.writeBigUInt64LE(BigInt(v), pos)
      this.memcpy(a as number[], 8, store, off)
    } else {
      this.memcpy(a as Array<bigint> | BigInt64Array, 8, this.buffer.writeBigUInt64LE, off, big)
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
