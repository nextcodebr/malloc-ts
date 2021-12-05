import { ReleaseOption } from '@/malloc/share'
import { PathLike } from 'fs'
import { copyOf, inflate, Pipe, Serializer } from '../../io'
import { LoadStorage, NewStorage, Storage } from '../../malloc'

export class BufferUnderflowError extends Error { }

export const checkUnderflow = (expected: number, got: number) => {
  if (got !== expected) {
    throw new BufferUnderflowError(`Buffer underflow: Expected ${expected} got ${got}`)
  }
}

export interface ISegment<K, V> {
  get: (hash: number, key: K, touch?: boolean) => V | null

  put: (hash: number, key: K, value: V, returnOld: boolean, onlyIfAbsent: boolean, map?: (k: K) => V) => V | null

  remove: (hash: number, key: K, returnOld: boolean) => V | null

  has: (hash: number, key: K) => boolean

  clear: () => number

  keys: () => Generator<K, void, unknown>

  values: () => Generator<V, void, unknown>

  entries: () => Generator<{
    key: K
    value: V
  }, void, unknown>

  size: number

  imageSize: number

  storeOn: (dst: Buffer) => void
}

export abstract class Segment<K, V> implements ISegment<K, V> {
  protected sz: number

  constructor () {
    this.sz = 0
  }

  get size () {
    return this.sz
  }

  abstract get imageSize (): number

  abstract storeOn (dst: Buffer): void

  abstract get (hash: number, key: K, touch?: boolean): V | null

  abstract put (hash: number, key: K, value: V, returnOld: boolean, onlyIfAbsent: boolean, map?: (k: K) => V): V | null

  abstract remove (hash: number, key: K, returnOld: boolean): V | null

  abstract has (hash: number, key: K): boolean

  abstract clear (): number

  protected abstract root (index: number): number

  protected abstract hash (p: number): number

  protected abstract left (p: number): number

  protected abstract right (p: number): number

  protected abstract parent (p: number): number

  protected abstract color (p: number): Color

  protected abstract setRoot (index: number, p: number): void

  protected abstract setHash (p: number, h: number): void

  protected abstract setLeft (p: number, v: number): void

  protected abstract setRight (p: number, v: number): void

  protected abstract setParent (p: number, v: number): void

  protected abstract setColor (p: number, v: Color): void

  protected abstract cloneEntry (p: number, parent: number, left: number, right: number, color: Color): number

  protected abstract free (p: number): void

  private colorOf (x: number) {
    return x === 0 ? Color.BLACK : this.color(x)
  }

  private parentOf (x: number) {
    return x === 0 ? 0 : this.parent(x)
  }

  private leftOf (x: number) {
    return x === 0 ? 0 : this.left(x)
  }

  private rightOf (x: number) {
    return x === 0 ? 0 : this.right(x)
  }

  protected fixAfterInsertion (p: number, ix: number) {
    this.setColor(p, Color.RED)

    while (p !== 0 && p !== this.root(ix) && this.color(this.parent(p)) === Color.RED) {
      if (this.parentOf(p) === this.leftOf(this.parentOf(this.parentOf(p)))) {
        const y = this.rightOf(this.parentOf(this.parentOf(p)))
        if (this.colorOf(y) === Color.RED) {
          this.setColor(this.parentOf(p), Color.BLACK)
          this.setColor(y, Color.BLACK)
          this.setColor(this.parentOf(this.parentOf(p)), Color.RED)
          p = this.parentOf(this.parentOf(p))
        } else {
          if (p === this.rightOf(this.parentOf(p))) {
            p = this.parentOf(p)
            this.rotateLeft(p, ix)
          }
          this.setColor(this.parentOf(p), Color.BLACK)
          this.setColor(this.parentOf(this.parentOf(p)), Color.RED)
          this.rotateRight(this.parentOf(this.parentOf(p)), ix)
        }
      } else {
        const y = this.leftOf(this.parentOf(this.parentOf(p)))
        if (this.colorOf(y) === Color.RED) {
          this.setColor(this.parentOf(p), Color.BLACK)
          this.setColor(y, Color.BLACK)
          this.setColor(this.parentOf(this.parentOf(p)), Color.RED)
          p = this.parentOf(this.parentOf(p))
        } else {
          if (p === this.leftOf(this.parentOf(p))) {
            p = this.parentOf(p)
            this.rotateRight(p, ix)
          }
          this.setColor(this.parentOf(p), Color.BLACK)
          this.setColor(this.parentOf(this.parentOf(p)), Color.RED)
          this.rotateLeft(this.parentOf(this.parentOf(p)), ix)
        }
      }
    }
    this.setColor(this.root(ix), Color.BLACK)
  }

  protected fixAfterDeletion (x: number, ix: number) {
    while (x !== this.root(ix) && this.colorOf(x) === Color.BLACK) {
      if (x === this.leftOf(this.parentOf(x))) {
        let sib = this.rightOf(this.parentOf(x))

        if (this.colorOf(sib) === Color.RED) {
          this.setColor(sib, Color.BLACK)
          this.setColor(this.parentOf(x), Color.RED)
          this.rotateLeft(this.parentOf(x), ix)
          sib = this.rightOf(this.parentOf(x))
        }

        if (this.colorOf(this.leftOf(sib)) === Color.BLACK && this.colorOf(this.rightOf(sib)) === Color.BLACK) {
          this.setColor(sib, Color.RED)
          x = this.parentOf(x)
        } else {
          if (this.colorOf(this.rightOf(sib)) === Color.BLACK) {
            this.setColor(this.leftOf(sib), Color.BLACK)
            this.setColor(sib, Color.RED)
            this.rotateRight(sib, ix)
            sib = this.rightOf(this.parentOf(x))
          }
          this.setColor(sib, this.colorOf(this.parentOf(x)))
          this.setColor(this.parentOf(x), Color.BLACK)
          this.setColor(this.rightOf(sib), Color.BLACK)
          this.rotateLeft(this.parentOf(x), ix)
          x = this.root(ix)
        }
      } else { // symmetric
        let sib = this.leftOf(this.parentOf(x))

        if (this.colorOf(sib) === Color.RED) {
          this.setColor(sib, Color.BLACK)
          this.setColor(this.parentOf(x), Color.RED)
          this.rotateRight(this.parentOf(x), ix)
          sib = this.leftOf(this.parentOf(x))
        }

        if (this.colorOf(this.rightOf(sib)) === Color.BLACK && this.colorOf(this.leftOf(sib)) === Color.BLACK) {
          this.setColor(sib, Color.RED)
          x = this.parentOf(x)
        } else {
          if (this.colorOf(this.leftOf(sib)) === Color.BLACK) {
            this.setColor(this.rightOf(sib), Color.BLACK)
            this.setColor(sib, Color.RED)
            this.rotateLeft(sib, ix)
            sib = this.leftOf(this.parentOf(x))
          }
          this.setColor(sib, this.colorOf(this.parentOf(x)))
          this.setColor(this.parentOf(x), Color.BLACK)
          this.setColor(this.leftOf(sib), Color.BLACK)
          this.rotateRight(this.parentOf(x), ix)
          x = this.root(ix)
        }
      }
    }

    this.setColor(x, Color.BLACK)
  }

  protected next (t: number) {
    let q
    if (t === 0) {
      return 0
    } else if ((q = this.right(t)) !== 0) {
      let p = q
      while ((q = this.left(p)) !== 0) {
        p = q
      }
      return p
    } else {
      let p = this.parent(t)
      q = t
      while (p !== 0 && q === this.right(p)) {
        q = p
        p = this.parent(p)
      }
      return p
    }
  }

  private rotateLeft (p: number, ix: number) {
    if (p !== 0) {
      const r = this.right(p)
      this.setRight(p, this.left(r))
      if (this.left(r) !== 0) {
        this.setParent(this.left(r), p)
      }
      this.setParent(r, this.parent(p))
      if (this.parent(p) === 0) {
        this.setRoot(ix, r)
      } else if (this.left(this.parent(p)) === p) {
        this.setLeft(this.parent(p), r)
      } else {
        this.setRight(this.parent(p), r)
      }
      this.setLeft(r, p)
      this.setParent(p, r)
    }
  }

  private rotateRight (p: number, ix: number) {
    if (p !== 0) {
      const l = this.left(p)
      this.setLeft(p, this.right(l))
      if (this.right(l) !== 0) {
        this.setParent(this.right(l), p)
      }
      this.setParent(l, this.parent(p))
      if (this.parent(p) === 0) {
        this.setRoot(ix, l)
      } else if (this.right(this.parent(p)) === p) {
        this.setRight(this.parent(p), l)
      } else {
        this.setLeft(this.parent(p), l)
      }
      this.setRight(l, p)
      this.setParent(p, l)
    }
  }

  protected deleteEntry (e: number, ix: number) {
    this.sz--
    let p = e
    let lp, rp
    if ((lp = this.left(p)) !== 0 && (rp = this.right(p)) !== 0) {
      const pp = this.parent(p)
      const s = this.next(p)
      // const sLen = keyLen(s)

      // Re-insert entry with successor values. We could evaluate if the successor's
      // payload could fit p's payload.
      const ne = this.cloneEntry(s, pp, lp, rp, this.color(p))

      if (pp !== 0) {
        if (this.left(pp) === p) {
          this.setLeft(pp, ne)
        } else if (this.right(pp) !== 0) {
          this.setRight(pp, ne)
        }
      }

      if (rp !== 0) {
        this.setParent(rp, ne)
      }
      if (lp !== 0) {
        this.setParent(lp, ne)
      }

      if (p === this.root(ix)) {
        this.setRoot(ix, ne)
      }

      this.free(p)

      p = s
      lp = this.left(p)
    } // p has 2 children

    // Start fixup at replacement node, if it exists.
    const replacement = lp !== 0 ? lp : this.right(p)

    if (replacement !== 0) {
      // Link replacement to parent
      this.setParent(replacement, this.parent(p))
      if (this.parent(p) === 0) {
        this.setRoot(ix, replacement)
      } else if (p === this.left(this.parent(p))) {
        this.setLeft(this.parent(p), replacement)
      } else {
        this.setRight(this.parent(p), replacement)
      }

      // Null out links so they are OK to use by fixAfterDeletion.
      // p.left = p.right = p.parent = null
      this.setLeft(p, 0)
      this.setRight(p, 0)
      this.setParent(p, 0)

      // Fix replacement
      if (this.color(p) === Color.BLACK) {
        this.fixAfterDeletion(replacement, ix)
      }
      this.free(p)
    } else if (this.parent(p) === 0) {
      this.free(p)
      this.setRoot(ix, 0)
    } else { // No children. Use self as phantom replacement and unlink.
      if (this.color(p) === Color.BLACK) {
        this.fixAfterDeletion(p, ix)
      }

      if (this.parent(p) !== 0) {
        if (p === this.left(this.parent(p))) {
          this.setLeft(this.parent(p), 0)
        } else if (p === this.right(this.parent(p))) {
          this.setRight(this.parent(p), 0)
        }
        this.setParent(p, 0)
      }
      this.free(p)
    }
  }

  protected abstract get tabLength (): number

  protected abstract readKey (p: number): K

  protected abstract readValue (p: number): V

  protected * nodes () {
    for (let ix = 0; ix < this.tabLength; ix++) {
      let e = this.getFirstEntry(this.root(ix))

      while (e) {
        yield e
        e = this.next(e)
      }
    }
  }

  public * keys () {
    for (const e of this.nodes()) {
      yield this.readKey(e)
    }
  }

  public * values () {
    for (const e of this.nodes()) {
      yield this.readValue(e)
    }
  }

  public * entries () {
    for (const e of this.nodes()) {
      const key = this.readKey(e)
      const value = this.readValue(e)
      yield {
        key,
        value
      }
    }
  }

  getFirstEntry (p: number) {
    if (p !== 0) {
      let l
      while ((l = this.left(p)) !== 0) {
        p = l
      }
    }
    return p
  }
}

export abstract class HashedSegment<K, V> extends Segment<K, V> {
  readonly options: number
  readonly table: Buffer
  readonly storage: Storage

  static baseLoad (src: PathLike, options: Options, copy = false) {
    const buf = inflate(src)
    const sz = buf.readUInt32LE(0)
    const tabLen = buf.readUInt32LE(4)
    const storeLen = buf.readUInt32LE(8)
    const table = copy ? copyOf(buf, 12, 12 + tabLen) : buf.slice(12, 12 + tabLen)
    checkUnderflow(tabLen, table.byteLength)
    const storage = LoadStorage(buf.slice(12 + tabLen, 12 + tabLen + storeLen), copy)
    checkUnderflow(storeLen, storage.imageSize)

    return {
      sz,
      table,
      storage,
      options
    }
  }

  constructor (cap: number, options: Options) {
    super()
    this.table = Buffer.alloc(4 * cap)
    this.storage = NewStorage(16)
    this.options = options
  }

  public get imageSize () {
    return 12 + this.table.byteLength + this.storage.imageSize
  }

  public storeOn (dst: Buffer): void {
    dst.writeUInt32LE(this.sz, 0)
    dst.writeUInt32LE(this.table.byteLength, 4)
    dst.writeUInt32LE(this.storage.imageSize, 8)
    dst = dst.slice(12)
    this.table.copy(dst)
    dst = dst.slice(this.table.byteLength)
    this.storage.storeOn(dst)
  }

  protected get tabLength () {
    return this.table.byteLength >>> 2
  }

  protected indexFor (hash: number) {
    return Math.abs(hash % this.tabLength)
  }

  protected metadaOverhead () {
    return this.timestamps() ? 4 : 0
  }

  protected timestamps () {
    return (this.options & Options.TIMESTAMPS) !== 0
  }

  protected root (index: number) {
    return this.table.readInt32LE(index << 2)
  }

  protected setRoot (index: number, offset: number) {
    this.table.writeInt32LE(offset, index << 2)
  }

  protected free (e: number) {
    if (!e || e < 0) {
      throw new Error(`Invalid pointer ${e}`)
    }
    this.storage.free(e)
  }

  public clear () {
    const sz = this.sz
    this.table.fill(0)
    this.storage.release(ReleaseOption.Physical)
    this.sz = 0
    return sz
  }
}

export enum Options {
  TIMESTAMPS = 0x1,
  HASH_THEN_BYTE_ORDER = 0x2,
  BYTE_ORDER = 0x4,
  HEAP = 0x8
}

export const enum Color {
  RED = 0,
  BLACK = 1
}

export const cmpNum = (l: number, r: number) => (l < r ? -1 : l > r ? 1 : 0)

export const serialize = <T> (ts: Serializer<T>, t: T) => {
  const sink = Pipe.sink.reset()
  ts.serialize(t, sink)
  return sink.unwrap()
}

export const probe = <K, V> (ks: Serializer<K>, vs: Serializer<V>, k: K, v: V) => {
  const sink = Pipe.sink.reset()
  ks.serialize(k, sink.skip(8))
  const p = sink.position
  sink.putInt32(0, p - 8)
  vs.serialize(v, sink)
  sink.putInt32(4, sink.position - p)

  return sink.unwrap()
}

export interface Comparable<K> {
  compareTo: (other: K) => number
}

export const cast = <T> (o: T) => (o as unknown) as Comparable<T>

const GENESIS = new Date().getTime()

const delta = () => new Date().getTime() - GENESIS

const deltaSeconds = () => delta() / 1000

export const now = deltaSeconds

export const check = <T> (key: T) => {
  if (key == null || key === undefined) {
    throw new Error('Key cannot be null')
  }
  return key
}
