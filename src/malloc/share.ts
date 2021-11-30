export type i32 = number
export type usize = number
export type offset = number

export const enum ReleaseOption {
  Logical = 1,
  Physical = 2
}

export type Mem = {
  offset: offset
  chunk: Buffer
}

export interface Allocator {
  maximumAddress: offset
  minimalSize: usize
  maxRequest: usize
  metadataLength: usize

  allocate: (size: usize) => offset

  free: (address: offset) => boolean

  expand: (moreBytes: usize) => void

  storeOn: (dst: Buffer) => void

  sizeOf: (address: offset) => usize

  clear: () => void
}

export interface Storage {

  imageSize: number

  reserved: number

  allocate: (size: usize) => offset

  getByte: (p: offset) => number

  getShort: (p: offset) => number

  getUShort: (p: offset) => number

  getInt: (p: offset) => i32

  getIntUnsafe: (p: offset) => i32

  getLongUnsafe: (p: offset) => bigint

  free: (address: offset) => boolean

  malloc: (size: usize) => Mem | null

  putByte: (p: offset, v: number) => void

  putShort: (p: offset, v: number) => void

  putUShort: (p: offset, v: number) => void

  putInt: (p: offset, v: i32) => void

  putIntUnsafe: (p: offset, v: i32) => void

  putLong: (p: offset, v: bigint) => void

  putLongUnsafe: (p: offset, v: bigint) => void

  onReleased: (p: offset) => void

  storeOn: (dst: Buffer) => void

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

  release: (option: ReleaseOption) => void
}
