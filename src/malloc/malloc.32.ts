/* esllet-disable @typescript-esllet/no-dupe-class-members */
import { MAX_SIGNED_32 } from '../32bit.math'

import { Allocator, Storage, offset, usize } from './share'

import { DLAssertions, AssertionError } from './assertions'

const VALIDATING = true

const SIZE_T_BITSIZE = 32
const SIZE_T_SIZE = SIZE_T_BITSIZE / 8
const SIZE_T_SIZE_X2 = 2 * SIZE_T_SIZE
const MALLOC_ALIGNMENT = 2 * SIZE_T_SIZE
const CHUNK_ALIGN_MASK = MALLOC_ALIGNMENT - 1
const MCHUNK_SIZE = 4 * SIZE_T_SIZE
const CHUNK_OVERHEAD = 2 * SIZE_T_SIZE
const MIN_CHUNK_SIZE = MCHUNK_SIZE + (CHUNK_ALIGN_MASK & ~CHUNK_ALIGN_MASK)
const MIN_REQUEST = MIN_CHUNK_SIZE - CHUNK_OVERHEAD - 1
const MAX_REQUEST = -MIN_CHUNK_SIZE << 2 & MAX_SIGNED_32

const PINUSE_BIT = 1
const CINUSE_BIT = 2
const FLAG4_BIT = 4
const INUSE_BITS = PINUSE_BIT | CINUSE_BIT
const FLAG_BITS = PINUSE_BIT | CINUSE_BIT | FLAG4_BIT

const NSMALLBINS = 32
const NTREEBINS = 32
const SMALLBIN_SHIFT = 3
const TREEBIN_SHIFT = 8
const MIN_LARGE_SIZE = 1 << TREEBIN_SHIFT
const MAX_SMALL_SIZE = MIN_LARGE_SIZE - 1
const MAX_SMALL_REQUEST = MAX_SMALL_SIZE - CHUNK_ALIGN_MASK - CHUNK_OVERHEAD
const TOP_FOOT_SIZE = 24

const padRequest = (req: number) => {
  return req + CHUNK_OVERHEAD + CHUNK_ALIGN_MASK & ~CHUNK_ALIGN_MASK
}

const okAddress = (n: number) => n >= 0

const chunkToMem = (p: number) => p + SIZE_T_SIZE_X2

const memToChunk = (p: number) => p - SIZE_T_SIZE_X2

const treeBinIndex = (s: number) => {
  const x = s >>> TREEBIN_SHIFT
  if (x === 0) {
    return 0
  } else if (x > 0xFFFF) {
    return NTREEBINS - 1
  } else {
    const k = 31 - x.numberOfLeadingZeros32()
    return (k << 1) + (s >>> k + TREEBIN_SHIFT - 1 & 1)
  }
}

const leftShiftForTreeIndex = (i: number) => {
  return i === NTREEBINS - 1 ? 0 : SIZE_T_BITSIZE - 1 - ((i >>> 1) + TREEBIN_SHIFT - 2)
}

const leftBits = (i: number) => {
  return i << 1 | -(i << 1)
}

const assert = (b: boolean, msg?: string) => {
  if (!b) {
    throw new AssertionError(msg ?? '')
  }
}

const okNext = (p: number, n: number) => {
  return p < n
}

const smallBinIndex = (s: number) => {
  return s >>> SMALLBIN_SHIFT
}

const isSmall = (s: number) => {
  return smallBinIndex(s) < NSMALLBINS
}

const smallBinIndexToSize = (i: number) => {
  return i << SMALLBIN_SHIFT
}

const isAligned = (a: number) => (a & CHUNK_ALIGN_MASK) === 0

export class DLAllocator32 implements Allocator {
  private readonly storage: Storage
  smallMap: number = 0
  designatedVictimSize: number = 0
  treeMap: number = 0
  topSize: number = 0
  top: number = 0
  occupied: number = 0
  designatedVictim: number = 0
  readonly treeBins: Int32Array
  readonly smallBins: Int32Array

  static metadataOverhead () {
    return (7 + NSMALLBINS + NTREEBINS) * 4
  }

  static load (src: Buffer, storage: Storage) {
    let off = 0
    const smallMap = src.readInt32LE(off)
    const designatedVictimSize = src.readInt32LE(off += 4)
    const treeMap = src.readInt32LE(off += 4)
    const topSize = src.readInt32LE(off += 4)
    const top = src.readInt32LE(off += 4)
    const occupied = src.readInt32LE(off += 4)
    const designatedVictim = src.readInt32LE(off += 4)

    const treeBins = new Int32Array(NTREEBINS)
    const smallBins = new Int32Array(NSMALLBINS)

    for (let i = 0; i < NTREEBINS; i++) {
      treeBins[i] = src.readInt32LE(off += 4)
    }

    for (let i = 0; i < NSMALLBINS; i++) {
      smallBins[i] = src.readInt32LE(off += 4)
    }

    return Object.setPrototypeOf({
      storage,
      smallMap,
      designatedVictimSize,
      treeMap,
      topSize,
      top,
      occupied,
      designatedVictim,
      treeBins,
      smallBins
    }, DLAllocator32.prototype)
  }

  constructor (storage: Storage) {
    this.storage = storage
    this.treeBins = new Int32Array(NTREEBINS)
    this.smallBins = new Int32Array(NSMALLBINS)
    this.clear()
  }

  private clear () {
    this.top = 0
    this.topSize = -TOP_FOOT_SIZE
    // head(top, topSize | PINUSE_BIT);
    this.designatedVictim = -1
    this.designatedVictimSize = 0

    for (let i = 0; i < NTREEBINS; i++) {
      this.treeBins[i] = -1
      this.clearTreeMap(i)
    }
    for (let i = 0; i < NSMALLBINS; i++) {
      this.smallBins[i] = -1
      this.clearSmallMap(i)
    }
    this.occupied = 0
  }

  public allocate (size: usize): offset {
    return this.dlmalloc(size)
  }

  public free (address: offset): boolean {
    return this.dlfree(address, true)
  }

  public getMaximumAddress (): offset {
    return MAX_SIGNED_32
  }

  public getMinimalSize (): usize {
    return 32
  }

  public expand (increase: usize): void {
    const next = this.topSize + increase

    if (next < 0 || next > MAX_SIGNED_32) {
      throw new AssertionError(`Request for increasing ${increase} bytes will overflow 32-bit capacity (topSize:${this.topSize} => ${next}) `)
    }

    this.topSize = next

    this.setHead(this.top, this.topSize | PINUSE_BIT)
    if (this.topSize >= 0) {
      this.checkTopChunk(this.top)
    }
  }

  public sizeOf (address: offset) {
    return this.chunkSize(memToChunk(address)) - SIZE_T_SIZE_X2
  }

  public storeOn (dst: Buffer) {
    let off = dst.writeInt32LE(this.smallMap)
    off = dst.writeInt32LE(this.designatedVictimSize, off)
    off = dst.writeInt32LE(this.treeMap, off)
    off = dst.writeInt32LE(this.topSize, off)
    off = dst.writeInt32LE(this.top, off)
    off = dst.writeInt32LE(this.occupied, off)
    off = dst.writeInt32LE(this.designatedVictim, off)

    for (let i = 0; i < NTREEBINS; i++) {
      off = dst.writeInt32LE(this.treeBins[i] ?? 0, off)
    }

    for (let i = 0; i < NSMALLBINS; i++) {
      off = dst.writeInt32LE(this.smallBins[i] ?? 0, off)
    }
  }

  public metadataLength () {
    return DLAllocator32.metadataOverhead()
  }

  private dlmalloc (bytes: number): number {
    const nb = bytes < MIN_REQUEST ? MIN_CHUNK_SIZE : padRequest(bytes)

    if (bytes <= MAX_SMALL_REQUEST) {
      let index = smallBinIndex(nb)

      const smallBits = this.smallMap >>> index

      if ((smallBits & 0x3) !== 0) {
        index += ~smallBits & 1

        return this.allocateFromSmallBin(index, nb)
      } else if (nb > this.designatedVictimSize) {
        if (smallBits !== 0) {
          return this.splitFromSmallBin((smallBits << index).numberOfTrailingZeros32(), nb)
        } else if (this.treeMap !== 0) {
          return this.splitSmallFromTree(nb)
        }
      }
    } else if (bytes > MAX_REQUEST) {
      return -1
    } else if (this.treeMap !== 0) {
      const mem = this.splitFromTree(nb)
      if (okAddress(mem)) {
        return mem
      }
    }

    if (nb <= this.designatedVictimSize) {
      return this.splitFromDesignatedVictim(nb)
    } else if (nb < this.topSize) {
      return this.splitFromTop(nb)
    }

    return -1
  }

  private dlfree (mem: number, shrink: boolean): boolean {
    let p = memToChunk(mem)

    if (okAddress(p) && this.isInUse(p)) {
      this.checkInUseChunk(p)
      let psize = this.chunkSize(p)
      this.occupied -= psize
      const next = p + psize

      if (!this.previousInUse(p)) {
        const previousSize = this.prevFoot(p)

        const previous = p - previousSize
        psize += previousSize
        p = previous
        if (okAddress(previous)) {
          if (p !== this.designatedVictim) {
            this.unlinkChunk(p, previousSize)
          } else if ((this.head(next) & INUSE_BITS) === INUSE_BITS) {
            this.designatedVictimSize = psize
            this.setFreeWithPreviousInUse(p, psize, next)
            return true
          }
        } else {
          throw new AssertionError()
        }
      }

      if (okNext(p, next) && this.previousInUse(next)) {
        if (!this.chunkInUse(next)) {
          if (next === this.top) {
            const tsize = this.topSize += psize
            this.top = p
            this.setHead(p, tsize | PINUSE_BIT)
            if (p === this.designatedVictim) {
              this.designatedVictim = -1
              this.designatedVictimSize = 0
            }
            if (shrink) {
              this.storage.onReleased(p + TOP_FOOT_SIZE)
            }
            return true
          } else if (next === this.designatedVictim) {
            const dsize = this.designatedVictimSize += psize
            this.designatedVictim = p
            this.setSizeAndPreviousInUseOfFreeChunk(p, dsize)
            return true
          } else {
            const nsize = this.chunkSize(next)
            psize += nsize
            this.unlinkChunk(next, nsize)
            this.setSizeAndPreviousInUseOfFreeChunk(p, psize)
            if (p === this.designatedVictim) {
              this.designatedVictimSize = psize
              return true
            }
          }
        } else {
          this.setFreeWithPreviousInUse(p, psize, next)
        }

        if (isSmall(psize)) {
          this.insertSmallChunk(p, psize)
        } else {
          this.insertLargeChunk(p, psize)
        }
      } else {
        DLAssertions.problemWithNext(psize, next, this.previousInUse(next))
      }
    } else {
      DLAssertions.notAllocated(mem)
    }
    return true
  }

  private setFreeWithPreviousInUse (p: number, s: number, n: number) {
    this.clearPreviousInUse(n)
    this.setSizeAndPreviousInUseOfFreeChunk(p, s)
  }

  private clearPreviousInUse (p: number) {
    this.setHead(p, this.head(p) & ~PINUSE_BIT)
  }

  private chunkInUse (p: number) {
    return (this.head(p) & CINUSE_BIT) !== 0
  }

  private unlinkChunk (p: number, s: number) {
    if (isSmall(s)) {
      this.unlinkSmallChunk(p, s)
    } else {
      this.unlinkLargeChunk(p)
    }
  }

  private unlinkSmallChunk (p: number, s: number) {
    const f = this.forward(p)
    const b = this.backward(p)

    const index = smallBinIndex(s)

    assert(!VALIDATING || this.chunkSize(p) === smallBinIndexToSize(index))

    if (f === p) {
      assert(!VALIDATING || b === p)
      this.clearSmallMap(index)
      this.smallBins[index] = -1
    } else if (okAddress(this.smallBins[index])) {
      if (this.smallBins[index] === p) {
        this.smallBins[index] = f
      }
      this.setForward(b, f)
      this.setBackward(f, b)
    } else {
      throw new AssertionError()
    }
  }

  private prevFoot (p: number) {
    return this.storage.getIntUnsafe(p)
  }

  private previousInUse (p: number) {
    return (this.head(p) & PINUSE_BIT) !== 0
  }

  private checkInUseChunk (p: number) {
    if (VALIDATING) {
      this.checkAnyChunk(p)
      if (!this.isInUse(p)) {
        DLAssertions.notInUse(p)
      }
      if (!this.nextPreviousInUse(p)) {
        DLAssertions.afterNotInUse(p)
      }
      /* If not pinuse previous chunk has OK offset */
      if (!this.previousInUse(p) && this.nextChunk(this.prevChunk(p)) !== p) {
        DLAssertions.nextIncorrect(p)
      }
    }
  }

  prevChunk (p: number): number {
    return p - this.prevFoot(p)
  }

  private nextPreviousInUse (p: number) {
    return (this.head(this.nextChunk(p)) & PINUSE_BIT) !== 0
  }

  private nextChunk (p: number): number {
    return p + this.chunkSize(p)
  }

  private checkAnyChunk (p: number) {
    if (VALIDATING) {
      if (!isAligned(chunkToMem(p))) {
        DLAssertions.unaligned(p, chunkToMem(p))
      }
      if (!okAddress(p)) {
        DLAssertions.invalidAddress(p)
      }
    }
  }

  private isInUse (p: number) {
    return (this.head(p) & INUSE_BITS) !== PINUSE_BIT
  }

  splitFromTop (nb: number): number {
    const rSize = this.topSize -= nb
    const p = this.top
    const r = this.top = p + nb
    this.setHead(r, rSize | PINUSE_BIT)

    this.setSizeAndPreviousInUseOfInUseChunk(p, nb)
    const mem = chunkToMem(p)

    this.checkTopChunk(this.top)
    this.checkMallocedChunk(mem, nb)

    return mem
  }

  private setSizeAndPreviousInUseOfInUseChunk (p: number, s: number) {
    this.setHead(p, s | PINUSE_BIT | CINUSE_BIT)
    this.setFoot(p, s)
    this.occupied += s
  }

  private setFoot (p: number, s: number) {
    this.setPrevFoot(p + s, s)
  }

  private setPrevFoot (p: number, value: number) {
    this.storage.putIntUnsafe(p, value)
  }

  private setHead (p: number, value: number): void {
    this.storage.putIntUnsafe(p + 4, value)
  }

  private head (p: number): number {
    return this.storage.getIntUnsafe(p + 4)
  }

  private checkMallocedChunk (mem: any, nb: number) {
    //
  }

  private checkTopChunk (top: number) {
    //
  }

  private splitFromDesignatedVictim (nb: number): number {
    const rsize = this.designatedVictimSize - nb
    const p = this.designatedVictim

    if (rsize >= MIN_CHUNK_SIZE) {
      const r = this.designatedVictim = p + nb
      this.designatedVictimSize = rsize
      this.setSizeAndPreviousInUseOfFreeChunk(r, rsize)
      this.setSizeAndPreviousInUseOfInUseChunk(p, nb)
    } else {
      const dvs = this.designatedVictimSize
      this.designatedVictimSize = 0
      this.designatedVictim = -1
      this.setInUseAndPreviousInUse(p, dvs)
    }

    const mem = chunkToMem(p)
    this.checkMallocedChunk(mem, nb)

    return mem
  }

  private setInUseAndPreviousInUse (p: number, s: number) {
    this.setSizeAndPreviousInUseOfInUseChunk(p, s)
    this.setHead(p + s, this.head(p + s) | PINUSE_BIT)
  }

  private setSizeAndPreviousInUseOfFreeChunk (p: number, s: number) {
    this.setHead(p, s | PINUSE_BIT)
    this.setFoot(p, s)
  }

  splitFromTree (nb: number): number {
    let v = -1
    let rsize = MAX_SIGNED_32 & -nb
    let t

    const index = treeBinIndex(nb)

    if ((t = this.treeBins[index]) !== -1) {
      let sizebits = nb << leftShiftForTreeIndex(index)
      let rst = -1
      for (; ;) {
        const trem = this.chunkSize(t) - nb
        if (trem >= 0 && trem < rsize) {
          v = t
          if ((rsize = trem) === 0) {
            break
          }
        }
        const rt = this.child(t, 1)
        t = this.child(t, sizebits >>> SIZE_T_BITSIZE - 1)
        if (rt !== -1 && rt !== t) {
          rst = rt
        }
        if (t === -1) {
          t = rst
          break
        }
        sizebits <<= 1
      }
    }
    if (t === -1 && v === -1) {
      const lb = leftBits(1 << index) & this.treeMap
      if (lb !== 0) {
        t = this.treeBins[lb.numberOfTrailingZeros32()]
      }
    }

    while (t !== -1) {
      const trem = this.chunkSize(t) - nb
      if (trem >= 0 && trem < rsize) {
        rsize = trem
        v = t
      }
      t = this.leftmostChild(t)
    }

    const designatedVictimFit = this.designatedVictimSize - nb
    if (v !== -1 && (designatedVictimFit < 0 || rsize < designatedVictimFit)) {
      if (okAddress(v)) { /* split */
        const r = v + nb
        assert(!VALIDATING || this.chunkSize(v) === rsize + nb)
        if (okNext(v, r)) {
          this.unlinkLargeChunk(v)
          if (rsize < MIN_CHUNK_SIZE) {
            this.setInUseAndPreviousInUse(v, rsize + nb)
          } else {
            this.setSizeAndPreviousInUseOfInUseChunk(v, nb)
            this.setSizeAndPreviousInUseOfFreeChunk(r, rsize)
            this.insertChunk(r, rsize)
          }
          return chunkToMem(v)
        }
      } else {
        throw new AssertionError()
      }
    }
    return -1
  }

  private insertChunk (p: number, s: number) {
    if (isSmall(s)) {
      this.insertSmallChunk(p, s)
    } else {
      this.insertLargeChunk(p, s)
    }
  }

  insertLargeChunk (x: number, s: number) {
    const index = treeBinIndex(s)
    const h = this.treeBins[index]

    this.setIndex(x, index)
    this.setChild(x, 0, -1)
    this.setChild(x, 1, -1)

    if (!this.treeMapIsMarked(index)) {
      this.markTreeMap(index)
      this.treeBins[index] = x
      this.setParent(x, -1)
      this.setForward(x, x)
      this.setBackward(x, x)
    } else {
      let t = h
      let k = s << leftShiftForTreeIndex(index)
      for (; ;) {
        if (this.chunkSize(t) !== s) {
          const childIndex = k >>> SIZE_T_BITSIZE - 1 & 1
          const child = this.child(t, childIndex)
          k <<= 1
          if (okAddress(child)) {
            t = child
          } else {
            this.setChild(t, childIndex, x)
            this.setParent(x, t)
            this.setForward(x, x)
            this.setBackward(x, x)
            break
          }
        } else {
          const f = this.forward(t)
          if (okAddress(t) && okAddress(f)) {
            this.setBackward(f, x)
            this.setForward(t, x)
            this.setForward(x, f)
            this.setBackward(x, t)
            this.setParent(x, -1)
            break
          } else {
            throw new AssertionError()
          }
        }
      }
    }
    this.checkFreeChunk(x)
  }

  private markTreeMap (index: number) {
    this.treeMap |= 1 << index
  }

  private treeMapIsMarked (index: number) {
    return (this.treeMap & 1 << index) !== 0
  }

  private setIndex (p: number, value: number) {
    this.storage.putIntUnsafe(p + 28, value)
  }

  private insertSmallChunk (p: number, s: number) {
    const index = smallBinIndex(s)

    const h = this.smallBins[index]

    if (!this.smallMapIsMarked(index)) {
      this.markSmallMap(index)
      this.smallBins[index] = p
      this.setForward(p, p)
      this.setBackward(p, p)
    } else if (okAddress(h)) {
      const b = this.backward(h)
      this.setForward(b, p)
      this.setForward(p, h)
      this.setBackward(h, p)
      this.setBackward(p, b)
    } else {
      throw new AssertionError()
    }
    this.checkFreeChunk(p)
  }

  markSmallMap (index: number) {
    this.smallMap |= 1 << index
  }

  smallMapIsMarked (index: number) {
    return (this.smallMap & 1 << index) !== 0
  }

  splitSmallFromTree (nb: number): number {
    const index = this.treeMap.numberOfTrailingZeros32()

    let t
    let v = t = this.treeBins[index]
    let rsize = this.chunkSize(t) - nb

    while ((t = this.leftmostChild(t)) !== -1) {
      const trem = this.chunkSize(t) - nb
      if (trem >= 0 && trem < rsize) {
        rsize = trem
        v = t
      }
    }

    if (okAddress(v)) {
      const r = v + nb
      assert(!VALIDATING || this.chunkSize(v) === rsize + nb)
      if (okNext(v, r)) {
        this.unlinkLargeChunk(v)
        if (rsize < MIN_CHUNK_SIZE) {
          this.setInUseAndPreviousInUse(v, rsize + nb)
        } else {
          this.setSizeAndPreviousInUseOfInUseChunk(v, nb)
          this.setSizeAndPreviousInUseOfFreeChunk(r, rsize)
          this.replaceDesignatedVictim(r, rsize)
        }
        const mem = chunkToMem(v)
        this.checkMallocedChunk(mem, nb)
        return mem
      } else {
        throw new AssertionError()
      }
    } else {
      throw new AssertionError()
    }
  }

  replaceDesignatedVictim (p: number, s: number) {
    const dvs = this.designatedVictimSize
    if (dvs !== 0) {
      const dv = this.designatedVictim
      assert(!VALIDATING || isSmall(dvs))
      this.insertSmallChunk(dv, dvs)
    }
    this.designatedVictimSize = s
    this.designatedVictim = p
  }

  splitFromSmallBin (index: number, nb: number): number {
    const h = this.smallBins[index]
    assert(!VALIDATING || this.chunkSize(h) === smallBinIndexToSize(index))

    const f = this.forward(h)
    const b = this.backward(h)

    if (f === h) {
      assert(!VALIDATING || b === h)
      this.clearSmallMap(index)
      this.smallBins[index] = -1
    } else {
      this.smallBins[index] = f
      this.setBackward(f, b)
      this.setForward(b, f)
    }

    const rsize = smallBinIndexToSize(index) - nb

    /* Fit here cannot be remainderless if 4byte sizes */
    if (rsize < MIN_CHUNK_SIZE) {
      this.setInUseAndPreviousInUse(h, smallBinIndexToSize(index))
    } else {
      this.setSizeAndPreviousInUseOfInUseChunk(h, nb)
      const r = h + nb
      this.setSizeAndPreviousInUseOfFreeChunk(r, rsize)
      this.replaceDesignatedVictim(r, rsize)
    }

    const mem = chunkToMem(h)
    this.checkMallocedChunk(mem, nb)

    return mem
  }

  private allocateFromSmallBin (index: number, nb: number): number {
    const h = this.smallBins[index]
    assert(!VALIDATING || this.chunkSize(h) === smallBinIndexToSize(index))

    const f = this.forward(h)
    const b = this.backward(h)

    if (f === h) {
      assert(!VALIDATING || b === h)
      this.clearSmallMap(index)
      this.smallBins[index] = -1
    } else {
      this.smallBins[index] = f
      this.setBackward(f, b)
      this.setForward(b, f)
    }

    this.setInUseAndPreviousInUse(h, smallBinIndexToSize(index))
    const mem = chunkToMem(h)
    this.checkMallocedChunk(mem, nb)

    return mem
  }

  private unlinkLargeChunk (x: number) {
    const xp = this.parent(x)
    let r
    if (this.backward(x) !== x) {
      const f = this.forward(x)
      r = this.backward(x)
      if (okAddress(f)) {
        this.setBackward(f, r)
        this.setForward(r, f)
      } else {
        throw new AssertionError()
      }
    } else {
      let rpIndex
      if ((r = this.child(x, rpIndex = 1)) !== -1 || (r = this.child(x, rpIndex = 0)) !== -1) {
        let rp = x
        while (true) {
          if (this.child(r, 1) !== -1) {
            rp = r
            rpIndex = 1
            r = this.child(r, 1)
          } else if (this.child(r, 0) !== -1) {
            rp = r
            rpIndex = 0
            r = this.child(r, 0)
          } else {
            break
          }
        }

        if (okAddress(rp)) {
          this.setChild(rp, rpIndex, -1)
        } else {
          throw new AssertionError()
        }
      }
    }

    const index = this.index(x)
    if (xp !== -1 || this.treeBins[index] === x) {
      const h = this.treeBins[index]
      if (x === h) {
        if ((this.treeBins[index] = r) === -1) {
          this.clearTreeMap(index)
        } else {
          this.setParent(r, -1)
        }
      } else if (okAddress(xp)) {
        if (this.child(xp, 0) === x) {
          this.setChild(xp, 0, r)
        } else {
          this.setChild(xp, 1, r)
        }
      } else {
        throw new AssertionError()
      }

      if (r !== -1) {
        if (okAddress(r)) {
          let c0, c1
          this.setParent(r, xp)
          if ((c0 = this.child(x, 0)) !== -1) {
            if (okAddress(c0)) {
              this.setChild(r, 0, c0)
              this.setParent(c0, r)
            } else {
              throw new AssertionError()
            }
          }
          if ((c1 = this.child(x, 1)) !== -1) {
            if (okAddress(c1)) {
              this.setChild(r, 1, c1)
              this.setParent(c1, r)
            } else {
              throw new AssertionError()
            }
          }
        } else {
          throw new AssertionError()
        }
      }
    }
  }

  private clearTreeMap (index: number) {
    this.treeMap &= ~(1 << index)
  }

  private clearSmallMap (index: number) {
    this.smallMap &= ~(1 << index)
  }

  private setParent (p: number, value: number) {
    this.storage.putIntUnsafe(p + 24, value)
  }

  private index (p: number) {
    return this.storage.getIntUnsafe(p + 28)
  }

  private setChild (p: number, index: number, value: number) {
    this.storage.putIntUnsafe(p + 16 + 4 * index, value)
  }

  private setForward (p: number, value: number) {
    this.storage.putIntUnsafe(p + 8, value)
  }

  private setBackward (p: number, value: number) {
    this.storage.putIntUnsafe(p + 12, value)
  }

  private forward (p: number) {
    return this.storage.getIntUnsafe(p + 8)
  }

  private backward (p: number) {
    return this.storage.getIntUnsafe(p + 12)
  }

  private parent (p: number) {
    return this.storage.getIntUnsafe(p + 24)
  }

  private leftmostChild (x: number): number {
    const left = this.child(x, 0)
    return left !== -1 ? left : this.child(x, 1)
  }

  private child (p: number, index: number): number {
    return this.storage.getIntUnsafe(p + 16 + 4 * index)
  }

  private chunkSize (p: number) {
    return this.head(p) & ~FLAG_BITS
  }

  private checkFreeChunk (x: number) {
    //    throw new Error('Method not implemented.')
  }
}
