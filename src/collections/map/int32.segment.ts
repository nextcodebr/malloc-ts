/* eslint-disable no-labels */

import { cmpNum, now, Options, Color, HashedSegment, serialize } from './segment'
import { Serializer, Pipe } from '../../io'
import { PathLike } from 'fs'

/**
 * Optimized for numeric keys. Data is embeded directly on tree nodes
 */
export class Int32Segment<V> extends HashedSegment<number, V> {
  readonly vs: Serializer<V>

  static load<R> (src: PathLike, vs: Serializer<R>, options: Options, copy = false) {
    const base = HashedSegment.baseLoad(src, options, copy)

    return Object.setPrototypeOf({
      ...base,
      vs
    }, Int32Segment.prototype)
  }

  constructor (vs: Serializer<V>, cap: number, options: Options) {
    super(cap, options)
    this.vs = vs
  }

  public get (hash: number, key: number, stamp?: boolean) {
    stamp = stamp && this.timestamps()

    const e = this.getEntry(key, this.indexFor(hash), stamp)

    return e > 0 ? this.readValue(e) : null
  }

  public put (hash: number, key: number, value: V, returnOld: boolean, onlyIfAbsent: boolean, map?: (k: number) => V): V | null {
    let rv: V | null = null

    const ix = this.indexFor(hash)
    let r = this.root(ix)

    if (r === 0) {
      if (map) {
        if (onlyIfAbsent) {
          this.setRoot(ix, this.newEntry(key, rv = map(key), hash, 0))
        }
      } else {
        this.setRoot(ix, this.newEntry(key, value, hash, 0))
      }
    } else {
      frame: {
        let c
        let parent
        do {
          parent = r
          c = this.cmp(r, key)
          if (c < 0) {
            r = this.left(r)
          } else if (c > 0) {
            r = this.right(r)
          } else {
            if (onlyIfAbsent) {
              rv = returnOld ? this.readValue(r) : null
            } else {
              value = map ? map(key) : value
              rv = map == null ? returnOld ? this.readValue(r) : null : value
              this.setValue(r, ix, value)
            }
            break frame
          }
        } while (r > 0)

        r = this.newEntry(key, map ? (rv = map(key)) : value, hash, parent)

        if (c < 0) {
          this.setLeft(parent, r)
        } else {
          this.setRight(parent, r)
        }

        this.fixAfterInsertion(r, ix)
      }
    }
    return rv
  }

  public remove (hash: number, key: number, returnOld: boolean): V | null {
    const ix = this.indexFor(hash)

    const e = this.getEntry(key, ix)
    if (!e) {
      return null
    }

    const ret = returnOld ? this.readValue(e) : null
    this.deleteEntry(e, ix)

    return ret
  }

  public has (hash: number, key: number) {
    const e = this.getEntry(key, this.indexFor(hash))

    return e > 0
  }

  protected cloneEntry (p: number, parent: number, left: number, right: number, color: Color) {
    const key = this.readKey(p)
    const vLen = this.maxValLen(p)
    const e = this.storage.allocate(this.baseEntrySize + vLen)
    this.setHash(e, this.hash(p))
    this.setKey(e, key)
    this.setLeft(e, left)
    this.setRight(e, right)
    this.setParent(e, parent)
    this.setColor(e, color)
    this.writeValue(e, this.storage.slice(p, this.baseEntrySize, this.maxValLen(p)))

    if (this.timestamps()) {
      this.stampNow(e)
    }

    return e
  }

  private getEntry (key: number, ix: number, stamp = false) {
    let e = this.root(ix)

    while (e > 0) {
      const c = this.cmp(e, key)
      if (c < 0) {
        e = this.left(e)
      } else if (c > 0) {
        e = this.right(e)
      } else {
        if (stamp) {
          this.stampNow(e)
        }
        return e
      }
    }

    return 0
  }

  private newEntry (key: number, v: V, hash: number, parent: number): number {
    const vbuf = serialize(this.vs, v)
    const vlen = vbuf.byteLength
    const e = this.storage.allocate(this.baseEntrySize + vlen)
    this.setKey(e, key)
    this.setHash(e, hash)
    this.setLeft(e, 0)
    this.setRight(e, 0)
    this.setParent(e, parent)
    this.setColor(e, Color.BLACK)
    this.writeValue(e, vbuf)

    if (this.timestamps()) {
      this.stampNow(e)
    }

    this.sz++

    return e
  }

  private stampOffset () {
    return 21
  }

  private writeValue (e: number, b: Buffer) {
    this.storage.write(e, this.baseEntrySize, b)
  }

  private stamp (e: number, v: number) {
    this.storage.putInt(e + this.stampOffset(), v)
  }

  private stampNow (e: number) {
    this.stamp(e, now())
  }

  protected readKey (p: number) {
    return this.storage.getInt(p)
  }

  protected hash (p: number) {
    return this.storage.getInt(p + 4)
  }

  protected left (p: number): number {
    return this.storage.getInt(p + 8)
  }

  protected right (p: number): number {
    return this.storage.getInt(p + 12)
  }

  protected parent (e: number) {
    return this.storage.getInt(e + 16)
  }

  protected color (e: number): Color {
    return this.storage.getByte(e + 20) ? Color.BLACK : Color.RED
  }

  private setKey (p: number, k: number) {
    this.storage.putInt(p, k)
  }

  protected setHash (p: number, h: number) {
    this.storage.putInt(p + 4, h)
  }

  protected setLeft (p: number, v: number) {
    this.storage.putInt(p + 8, v)
  }

  protected setRight (p: number, val: number): void {
    this.storage.putInt(p + 12, val)
  }

  protected setParent (e: number, val: number) {
    this.storage.putInt(e + 16, val)
  }

  protected setColor (e: number, val: Color) {
    if (e > 0) { // guard due to unconditional call in fixAfterInsertion
      this.storage.putByte(e + 20, val)
    }
  }

  private cmp (p: number, key: number) {
    return cmpNum(key, this.readKey(p))
  }

  protected readValue (p: number): V {
    return this.vs.deserialize(Pipe.shared.source(this.storage.slice(p, this.baseEntrySize, this.maxValLen(p))))
  }

  private setValue (e: number, ix: number, val: V) {
    const v = serialize(this.vs, val)

    if (v.byteLength <= this.maxValLen(e)) {
      this.writeValue(e, v)
      if (this.timestamps()) {
        this.stampNow(e)
      }
    } else {
      const p = this.parent(e)
      const le = this.left(e)
      const re = this.right(e)
      const ne = this.replacementEntry(e, v, p, le, re)

      if (p !== 0) {
        if (this.left(p) === e) {
          this.setLeft(p, ne)
        } else if (this.right(p) === e) {
          this.setRight(p, ne)
        }
      }

      if (le !== 0) {
        this.setParent(le, ne)
      }
      if (re !== 0) {
        this.setParent(re, ne)
      }

      if (e === this.root(ix)) {
        this.setRoot(ix, ne)
      }

      this.free(e)
    }
  }

  private get baseEntrySize () {
    return 21 + this.metadaOverhead()
  }

  private replacementEntry (p: number, v: Buffer, parent: number, left: number, right: number) {
    const key = this.readKey(p)
    const hash = this.hash(p)
    const color = this.color(p)
    const e = this.storage.allocate(this.baseEntrySize + v.byteLength)
    this.setKey(e, key)
    this.setHash(e, hash)
    this.setLeft(e, left)
    this.setRight(e, right)
    this.setParent(e, parent)
    this.setColor(e, color)
    this.writeValue(e, v)

    if (this.timestamps()) {
      this.stampNow(e)
    }

    return e
  }

  private maxValLen (e: number) {
    return this.storage.sizeOf(e) - this.baseEntrySize
  }
}
