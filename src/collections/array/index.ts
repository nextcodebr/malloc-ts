import { Mem, LoadStorage, NewStorage, Storage } from '../../malloc'
import { inflate, isBuffer, newLength, Pipe, Serializer, sync } from '../../io'
import { PathLike } from 'fs'

const failed = (len: number) => {
  return new Error(`Failed to allocate ${len} bytes`)
}

const alignLength = (length: number, alignment: number) => {
  const chunks = Math.floor(length / alignment) + ((length % alignment === 0) ? 0 : 1)

  return chunks * alignment
}

export class GrowableArray<T> {
  private size: number
  private readonly serializer: Serializer<T>
  private readonly storage: Storage
  private mem: Mem

  public static load<V> (src: PathLike, serializer: Serializer<V>, copy = false): GrowableArray<V> {
    const buf = inflate(src)
    const size = buf.readUInt32LE(0)
    const offset = buf.readUInt32LE(4)
    const length = buf.readUInt32LE(8)
    const storage = LoadStorage(buf.slice(12), copy)

    const mem: Mem = {
      offset,
      chunk: storage.slab(offset, length)
    }

    return Object.setPrototypeOf({
      size,
      storage,
      serializer,
      mem
    }, GrowableArray.prototype)
  }

  constructor (capacity: number, serializer: Serializer<T>, avgObjSize?: number) {
    this.size = 0
    this.serializer = serializer
    capacity = alignLength(capacity, 128)
    const initial = capacity * (4 + (avgObjSize ?? 16))
    this.storage = NewStorage(initial)
    const mem = this.storage.malloc(4 * capacity)
    if (!mem) {
      throw failed(initial)
    }
    this.mem = mem
  }

  public get length () {
    return this.size
  }

  public get capacity () {
    return this.mem.chunk.byteLength >> 2
  }

  private writeOffset (index: number, p: number) {
    this.mem.chunk.writeInt32LE(p, index << 2)
  }

  private readOffset (index: number) {
    return this.mem.chunk.readInt32LE(index << 2)
  }

  public get (index: number): T | undefined {
    if (index < 0 || index >= this.size) {
      return undefined
    }

    const p = this.readOffset(index)

    return this.serializer.deserialize(Pipe.shared.source(this.storage.slab(p)))
  }

  public set (index: number, value: T) {
    if (index >= this.capacity) {
      this.grow(index + 1)
    }

    const dst = Pipe.sink.reset()
    this.serializer.serialize(value, dst)
    const required = dst.position

    let p = this.mem.chunk.readInt32LE(index << 2)

    if (p > 0) {
      const size = this.storage.sizeOf(p)
      if (size < required) {
        this.storage.free(p)
        p = this.storage.allocate(required)
      }
    } else {
      p = this.storage.allocate(required)
    }

    if (p < 0) {
      throw failed(required)
    }

    dst.drainTo(this.storage.slab(p, required))
    this.writeOffset(index, p)

    if (index >= this.size) {
      this.size = index + 1
    }
  }

  private grow (req: number) {
    const cap = this.capacity
    req = alignLength(req, 16)
    const nextLen = newLength(cap, req, cap >> 1)
    const next = this.storage.malloc(4 * nextLen)
    if (!next) {
      throw failed(cap)
    }
    this.mem.chunk.copy(next.chunk)
    this.storage.free(this.mem.offset)

    this.mem = next
  }

  push (value: T) {
    this.set(this.size, value)
  }

  pop (): T | undefined {
    const sz = this.size
    if (sz <= 0) {
      return undefined
    }
    const p = this.readOffset(sz - 1)
    const rv = this.get(this.size - 1)
    this.storage.free(p)
    this.size--

    return rv
  }

  poll (): T | undefined {
    const sz = this.size

    if (sz <= 0) {
      return undefined
    }

    const p = this.readOffset(0)
    const rv = this.get(0)
    this.storage.free(p)
    this.mem.chunk.copy(this.mem.chunk, 0, 4)
    this.size--

    return rv
  }

  storeOn (dst: Buffer) {
    dst.writeUInt32LE(this.size, 0)
    dst.writeUInt32LE(this.mem.offset, 4)
    dst.writeUInt32LE(this.mem.chunk.byteLength, 8)
    this.storage.storeOn(dst.slice(12))
  }

  serialize () {
    const dst = Buffer.allocUnsafe(12 + this.storage.imageSize)
    this.storeOn(dst)
    return dst
  }

  public saveOn (dst: PathLike) {
    if (isBuffer(dst)) {
      this.storeOn(dst as Buffer)
    } else {
      sync(this.serialize(), dst as string)
    }
  }

  public get imageSize () {
    return 12 + this.storage.imageSize
  }
}
