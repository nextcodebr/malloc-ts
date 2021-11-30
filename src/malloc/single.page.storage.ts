import { Allocator, Mem, offset, ReleaseOption, Storage } from './share'
import { DLAllocator32 } from './malloc.32'
import { logg, Level } from '../log'
import { PathLike } from 'fs'
import { copyOf, inflate } from '../io'
import process from 'process'

const SMALL_PAGE_SIZE = 0x1000
const REALLOC_CHUNK = 1024 * SMALL_PAGE_SIZE
const REALLOC_CHUNK_MASK = REALLOC_CHUNK - 1

const align = (request: number) => {
  return request < REALLOC_CHUNK ? REALLOC_CHUNK : REALLOC_CHUNK * (Math.floor(request / REALLOC_CHUNK) + ((request & REALLOC_CHUNK_MASK) === 0 ? 0 : 1))
}

const EMPTY = Buffer.alloc(0)

const seqId = () => {
  // return BigInt(new Date().getTime())
  const [s, ns] = process.hrtime()
  return BigInt(s) * 1000n * 1000n * 1000n + BigInt(ns)
}

export class SinglePageStorage implements Storage {
  readonly id: bigint
  readonly allocator: Allocator
  mem: Buffer

  public static load (src: PathLike, copy = true) {
    let buffer = inflate(src)
    const id = buffer.readBigInt64LE(0)
    buffer = buffer.slice(8)
    const larval: any = Object.setPrototypeOf({ id }, SinglePageStorage.prototype)

    const allocator = DLAllocator32.load(buffer, larval as SinglePageStorage)

    let mem = buffer.slice(allocator.metadataLength)
    if (copy) {
      mem = copyOf(mem)
    }

    larval.allocator = allocator
    larval.mem = mem

    return larval as SinglePageStorage
  }

  constructor (initialSize: number) {
    this.id = seqId()
    this.allocator = new DLAllocator32(this)
    this.mem = EMPTY
    this.expand(initialSize)
  }

  private expand (request: number) {
    const size = this.mem.byteLength
    const newSize = align(size + request)

    logg(`#${this.id}: Expanding ${this.mem.byteLength} to ${newSize}`, Level.DEBUG)

    const next = Buffer.alloc(newSize)

    if (this.mem.byteLength) {
      this.mem.copy(next)
    }
    this.mem = next

    this.allocator.expand(newSize - size)

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

  public allocate (bytes: number): number {
    let rv: number
    if (bytes > this.allocator.maxRequest) {
      rv = -1
    } else {
      rv = this.allocator.allocate(bytes)

      if (rv < 0) {
        this.expand(bytes)
        rv = this.allocator.allocate(bytes)
      }
    }

    return rv
  }

  public malloc (bytes: number): Mem | null {
    const offset = this.allocate(bytes)

    if (offset < 0) {
      return null
    }

    return {
      offset,
      chunk: this.mem.slice(offset, offset + bytes)
    }
  }

  public free (address: number): boolean {
    return this.allocator.free(address)
  }

  public slice (p: offset, off = 0, length?: number): Buffer {
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

  public get reserved (): number {
    return this.mem.byteLength
  }

  public get imageSize () {
    return 8 + this.reserved + this.allocator.metadataLength
  }

  public storeOn (dst: Buffer) {
    dst.writeBigInt64LE(this.id)
    dst = dst.slice(8)
    this.allocator.storeOn(dst)
    this.mem.copy(dst, this.allocator.metadataLength)
  }

  public write (p: number, off: number, src: Buffer) {
    const sz = this.sizeOf(p)
    if (off < 0 || (off + src.byteLength) > sz) {
      throw new Error(`Invalid memory access: ${off}+${src.byteLength} > ${p}+${sz}`)
    }
    src.copy(this.mem, p + off, 0, src.byteLength)
  }

  public release (option: ReleaseOption) {
    const res = this.reserved
    if (res) {
      this.allocator.clear()

      if (option === ReleaseOption.Physical) {
        this.mem = EMPTY
      } else {
        this.allocator.expand(res)
      }
    }
  }
}
