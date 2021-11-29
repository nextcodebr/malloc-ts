export type i32 = number
export type usize = number
export type offset = number

export type Mem = {
  offset: offset
  chunk: Buffer
}

export interface Allocator {
  allocate: (size: usize) => offset

  free: (address: offset) => boolean

  getMaximumAddress: () => offset

  getMinimalSize: () => usize

  expand: (moreBytes: usize) => void

  save: (dst: Buffer) => void

  sizeOf: (address: offset) => usize

  metadataLength: () => usize
}

export interface Storage {
  allocate: (size: usize) => offset

  getByte: (p: offset) => number

  getShort: (p: offset) => number

  getUShort: (p: offset) => number

  getInt: (p: offset) => i32

  getIntUnsafe: (p: offset) => i32

  getLongUnsafe: (p: offset) => bigint

  free: (address: offset) => boolean

  imageSize: () => number

  malloc: (size: usize) => Mem | null

  putByte: (p: offset, v: number) => void

  putShort: (p: offset, v: number) => void

  putUShort: (p: offset, v: number) => void

  putInt: (p: offset, v: i32) => void

  putIntUnsafe: (p: offset, v: i32) => void

  putLong: (p: offset, v: bigint) => void

  putLongUnsafe: (p: offset, v: bigint) => void

  onReleased: (p: offset) => void

  reserved: () => usize

  save: (dst: Buffer) => void

  sizeOf: (address: offset) => usize

  /**
   * Fetches a slice of memory with base address p and offset off (relative) to p with up to length bytes.
   *
   * @param {offset} p - base address
   * @param {number} off - offset, relative from p
   * @param {number} length - number of bytes to request. If not provided, will be computed as sizeOf(p)
   */
  slice: (p: offset, off: number, length?: usize) => Buffer

  slab: (p: offset, length?: usize) => Buffer

  write: (p: offset, off: number, b: Buffer) => void
}
