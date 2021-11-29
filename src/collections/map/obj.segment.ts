/* eslint-disable no-labels */
import { cast, cmpNum, now, Options, probe, Color, HashedSegment, serialize } from './segment'
import { Serializer, Pipe } from '../../io'
import { PathLike } from 'fs'

export class ObjSegment<K, V> extends HashedSegment<K, V> {
  readonly ks: Serializer<K>
  readonly vs: Serializer<V>

  static load<L, R> (src: PathLike, ks: Serializer<L>, vs: Serializer<R>, options: Options, copy = false) {
    const base = HashedSegment.baseLoad(src, options, copy)

    return Object.setPrototypeOf({
      ...base,
      ks,
      vs
    }, ObjSegment.prototype)
  }

  constructor (ks: Serializer<K>, vs: Serializer<V>, cap: number, options: Options) {
    super(cap, options)
    this.ks = ks
    this.vs = vs
  }

  public get (hash: number, key: K, stamp?: boolean) {
    stamp = stamp && this.timestamps()

    const e = this.getEntry(key, this.indexFor(hash), hash, stamp)

    return e > 0 ? this.readValue(e) : null
  }

  public put (hash: number, key: K, value: V, returnOld: boolean, onlyIfAbsent: boolean, map?: (k: K) => V): V | null {
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
      const mode = this.cmpMode
      frame: {
        let c
        let parent
        const probe = serialize(this.ks, key)

        do {
          parent = r
          c = this.cmp(mode, r, key, probe, hash)
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

  public remove (hash: number, key: K, returnOld: boolean): V | null {
    const ix = this.indexFor(hash)

    const e = this.getEntry(key, ix, hash)
    if (!e) {
      return null
    }

    const ret = returnOld ? this.readValue(e) : null
    this.deleteEntry(e, ix)

    return ret
  }

  private get baseEntrySize () {
    return 19 + this.metadaOverhead()
  }

  protected cloneEntry (p: number, parent: number, left: number, right: number, color: Color) {
    const len = this.keyLen(p)
    const vLen = this.maxValLen(p)
    const e = this.storage.allocate(this.baseEntrySize + len + vLen)
    this.setHash(e, this.hash(p))
    this.setKeyLen(e, len)
    this.setLeft(e, left)
    this.setRight(e, right)
    this.setParent(e, parent)
    this.setColor(e, color)
    this.writeKey(e, this.keySlice(p))
    this.writeValue(e, this.storage.slice(p, this.valOffset(p), this.maxValLen(p)))

    if (this.timestamps()) {
      this.stampNow(e)
    }

    return e
  }

  private get cmpMode () {
    return this.options & ~(Options.TIMESTAMPS)
  }

  private getEntry (key: K, ix: number, hash: number, stamp = false) {
    let e = this.root(ix)
    const probe = serialize(this.ks, key)
    const mode = this.cmpMode

    while (e > 0) {
      const c = this.cmp(mode, e, key, probe, hash)
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

  private newEntry (k: K, v: V, hash: number, parent: number): number {
    const kv = probe(this.ks, this.vs, k, v)

    const lim = kv.byteLength
    const e = this.storage.allocate(this.baseEntrySize + lim - 8)
    const keyLen = kv.readInt32LE(0)
    const valLen = kv.readInt32LE(4)

    this.setHash(e, hash)
    this.setKeyLen(e, keyLen)
    this.setLeft(e, 0)
    this.setRight(e, 0)
    this.setParent(e, parent)
    this.setColor(e, Color.BLACK)

    this.writeKey(e, kv.slice(8, 8 + keyLen))
    this.writeValue(e, kv.slice(8 + keyLen, 8 + keyLen + valLen))

    if (this.timestamps()) {
      this.stampNow(e)
    }

    this.sz++

    return e
  }

  private stampOffset () {
    return 19
  }

  private writeValue (e: number, b: Buffer) {
    this.storage.write(e, this.valOffset(e), b)
  }

  private stamp (e: number, v: number) {
    this.storage.putInt(e + this.stampOffset(), v)
  }

  private stampNow (e: number) {
    this.stamp(e, now())
  }

  private valOffset (e: number) {
    return this.baseEntrySize + this.keyLen(e)
  }

  private keyLen (e: number) {
    return this.storage.getUShort(e + 4)
  }

  private writeKey (e: number, b: Buffer) {
    this.storage.write(e, this.baseEntrySize, b)
  }

  private setKeyLen (p: number, val: number) {
    this.storage.putUShort(p + 4, val)
  }

  private cmp (cmp: Options, p: number, heap: K, bin: Buffer, hash: number) {
    let rv

    switch (cmp) {
      case Options.HASH_THEN_BYTE_ORDER:
        rv = cmpNum(hash, this.hash(p))
        if (rv === 0) {
          rv = bin.compare(this.keySlice(p))
        }
        break
      case Options.BYTE_ORDER:
        rv = bin.compare(this.keySlice(p))
        break
      default:
        rv = cast(heap).compareTo(this.readKey(p))
        break
    }

    return rv
  }

  protected readKey (p: number): K {
    return this.ks.deserialize(Pipe.shared.source(this.storage.slice(p, this.baseEntrySize, this.keyLen(p))))
  }

  protected readValue (p: number): V {
    const off = this.valOffset(p)

    return this.vs.deserialize(Pipe.shared.source(this.storage.slice(p, off, this.maxValLen(p))))
  }

  protected hash (p: number) {
    return this.storage.getInt(p)
  }

  protected left (p: number): number {
    return this.storage.getInt(p + 6)
  }

  protected right (p: number): number {
    return this.storage.getInt(p + 10)
  }

  protected parent (e: number) {
    return this.storage.getInt(e + 14)
  }

  protected color (e: number): Color {
    return this.storage.getByte(e + 18) ? Color.BLACK : Color.RED
  }

  protected setHash (p: number, h: number) {
    this.storage.putInt(p, h)
  }

  protected setLeft (p: number, v: number) {
    this.storage.putInt(p + 6, v)
  }

  protected setParent (e: number, val: number) {
    this.storage.putInt(e + 14, val)
  }

  protected setColor (e: number, val: Color) {
    if (e > 0) {
      this.storage.putByte(e + 18, val)
    }
  }

  protected setRight (p: number, val: number): void {
    this.storage.putInt(p + 10, val)
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
      const ne = this.replacementEntry(e, v, this.hash(e), p, le, re, this.color(e))

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

  private replacementEntry (p: number, v: Buffer, hash: number, parent: number, left: number, right: number, color: Color) {
    const len = this.keyLen(p)
    const e = this.storage.allocate(this.baseEntrySize + len + v.byteLength)
    this.setHash(e, hash)
    this.setKeyLen(e, len)
    this.setLeft(e, left)
    this.setRight(e, right)
    this.setParent(e, parent)
    this.setColor(e, color)
    this.writeKey(e, this.keySlice(p))
    this.writeValue(e, v)

    if (this.timestamps()) {
      this.stampNow(e)
    }

    return e
  }

  private keySlice (p: number) {
    return this.storage.slice(p, this.baseEntrySize, this.keyLen(p))
  }

  private maxValLen (e: number) {
    return this.storage.sizeOf(e) - this.valOffset(e)
  }
}
