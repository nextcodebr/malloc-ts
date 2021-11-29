import { Allocator, Mem, offset, Storage } from './share'
import { DLAllocator32 } from './malloc.32'

const SMALL_PAGE_SIZE = 0x1000
const REALLOC_CHUNK = 1024 * SMALL_PAGE_SIZE
const REALLOC_CHUNK_MASK = REALLOC_CHUNK - 1

const align = (request: number) => {
  return request < REALLOC_CHUNK ? REALLOC_CHUNK : REALLOC_CHUNK * (request / REALLOC_CHUNK + ((request & REALLOC_CHUNK_MASK) === 0 ? 0 : 1))
}

const EMPTY = Buffer.alloc(0)

export class SinglePageStorage implements Storage {
  readonly allocator: Allocator
  mem: Buffer

  constructor (initialSize: number) {
    this.allocator = new DLAllocator32(this)
    this.mem = EMPTY
    this.expand(initialSize)
  }

  private expand (request: number) {
    const aligned = align(request)
    const newSize = align(this.mem.byteLength + request)
    const next = Buffer.alloc(newSize)

    if (this.mem.byteLength) {
      this.mem.copy(next)
    }
    this.mem = next

    this.allocator.expand(aligned)

    return next
  }

  getByte (p: number): number {
    return this.mem.readInt8(p)
  }

  getShort (p: number): number {
    return this.mem.readInt16LE(p)
  }

  getUShort (p: number): number {
    return this.mem.readUInt16LE(p)
  }

  getInt (p: number): number {
    return this.mem.readInt32LE(p)
  }

  getIntUnsafe (p: number): number {
    return this.mem.readInt32LE(p)
  }

  getLongUnsafe (p: number): bigint {
    return this.mem.readBigInt64LE(p)
  }

  putByte (p: offset, v: number): void {
    this.mem.writeInt8(v, p)
  }

  putShort (p: number, v: number): void {
    this.mem.writeInt16LE(v, p)
  }

  putUShort (p: number, v: number): void {
    this.mem.writeUInt16LE(v, p)
  }

  putInt (p: number, v: number): void {
    this.mem.writeInt32LE(v, p)
  }

  putIntUnsafe (p: number, v: number): void {
    this.mem.writeInt32LE(v, p)
  }

  putLong (p: number, v: bigint): void {
    this.mem.writeBigInt64LE(v, p)
  }

  putLongUnsafe (p: number, v: bigint): void {
    this.mem.writeBigInt64LE(v, p)
  }

  public sizeOf (address: offset) {
    return this.allocator.sizeOf(address)
  }

  onReleased (p: number): void {

  }

  allocate (bytes: number): number {
    let rv = this.allocator.allocate(bytes)

    if (rv < 0) {
      this.expand(bytes)
      rv = this.allocator.allocate(bytes)
    }

    return rv
  }

  malloc (bytes: number): Mem | null {
    const offset = this.allocate(bytes)

    if (offset < 0) {
      return null
    }

    return {
      offset,
      chunk: this.mem.slice(offset, offset + bytes)
    }
  }

  free (address: number): boolean {
    return this.allocator.free(address)
  }

  slice (p: offset, off = 0, length?: number): Buffer {
    const sz = this.sizeOf(p)
    off = off ?? 0
    length = length ?? sz
    length += off

    if (off < 0) {
      throw new Error(`Offset ${off} < 0`)
    }

    if (length < 0 || length > sz) {
      throw new Error(`Invalid memory access. Allowed: [${p},${p + sz}). Requested: [${p},${p + length})`)
    }

    return this.mem.slice(p + off, p + length)
  }

  slab (p: offset, length?: number): Buffer {
    return this.slice(p, 0, length)
  }

  reserved (): number {
    return this.mem.byteLength
  }

  imageSize () {
    return this.reserved()
  }

  save (dst: Buffer) {
    this.allocator.save(dst)
    this.mem.copy(dst, this.allocator.metadataLength())
  }

  write (p: number, off: number, src: Buffer) {
    const sz = this.sizeOf(p)
    if (off < 0 || (off + src.byteLength) > sz) {
      throw new Error(`Invalid memory access: ${off}+${src.byteLength} > ${p}+${sz}`)
    }
    src.copy(this.mem, p + off, 0, src.byteLength)
  }

  static load (src: Buffer) {
    const opaque: any = Object.setPrototypeOf({}, SinglePageStorage.prototype)

    const rv = opaque as SinglePageStorage

    const allocatror = DLAllocator32.load(src, rv)
    opaque.allocator = allocatror
    opaque.mem = src.slice(allocatror.metadataOverhead())

    return rv
  }
}
