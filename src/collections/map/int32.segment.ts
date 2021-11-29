/* eslint-disable no-labels */

import { ISegment, Options, Color, serialize, now, cmpNum } from './segment'
import { Storage, NewStorage } from '../../malloc'
import { Serializer, Pipe } from '../../io'

/**
 * Optimized for numeric keys. Data is embeded directly on tree nodes
 */
export class Int32Segment<V> implements ISegment<number, V> {
  readonly options: number
  readonly vs: Serializer<V>
  readonly table: Buffer
  readonly storage: Storage
  sz: number

  constructor (vs: Serializer<V>, cap: number, options: Options) {
    this.vs = vs
    this.table = Buffer.alloc(4 * cap)
    this.storage = NewStorage(16)
    this.options = options
    this.sz = 0
  }

  public get (hash: number, key: number, stamp?: boolean) {
    stamp = stamp && this.timestamps()

    const e = this.getEntry(key, this.indexFor(hash), hash, stamp)

    return e > 0 ? this.readVal(e) : null
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
              rv = returnOld ? this.readVal(r) : null
            } else {
              value = map ? map(key) : value
              rv = map == null ? returnOld ? this.readVal(r) : null : value
              this.setValue(r, ix, key, value)
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

    const e = this.getEntry(key, ix, hash)
    if (!e) {
      return null
    }

    const ret = returnOld ? this.readVal(e) : null
    this.deleteEntry(e, ix)

    return ret
  }

  private next (t: number) {
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

  private replEntry (p: number, parent: number, left: number, right: number, color: Color) {
    const key = this.key(p)
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

  private deleteEntry (e: number, ix: number) {
    const {
      color, left, right, parent, next, root,
      setLeft, setRight, setRoot, setParent
    } = this

    this.sz--
    let p = e
    let lp, rp
    if ((lp = left(p)) !== 0 && (rp = right(p)) !== 0) {
      const pp = parent(p)
      const s = next(p)
      // const sLen = keyLen(s)

      // Re-insert entry with successor values. We could evaluate if the successor's
      // payload could fit p's payload.
      const ne = this.replEntry(s, pp, lp, rp, this.color(p))
      // right(ne, rp)
      // left(ne, lp)

      if (pp !== 0) {
        if (left(pp) === p) {
          setLeft(pp, ne)
        } else if (right(pp) !== 0) {
          setRight(pp, ne)
        }
      }

      if (rp !== 0) {
        setParent(rp, ne)
      }
      if (lp !== 0) {
        setParent(lp, ne)
      }

      if (p === root(ix)) {
        setRoot(ix, ne)
      }

      this.free(p)

      p = s
      lp = left(p)
    } // p has 2 children

    // Start fixup at replacement node, if it exists.
    const replacement = lp !== 0 ? lp : right(p)

    if (replacement !== 0) {
      // Link replacement to parent
      setParent(replacement, parent(p))
      if (parent(p) === 0) {
        setRoot(ix, replacement)
      } else if (p === left(parent(p))) {
        setLeft(parent(p), replacement)
      } else {
        setRight(parent(p), replacement)
      }

      // Null out links so they are OK to use by fixAfterDeletion.
      // p.left = p.right = p.parent = null
      setLeft(p, 0)
      setRight(p, 0)
      setParent(p, 0)

      // Fix replacement
      if (color(p) === Color.BLACK) {
        this.fixAfterDeletion(replacement, ix)
      }
      this.free(p)
    } else if (parent(p) === 0) {
      this.free(p)
      setRoot(ix, 0)
    } else { // No children. Use self as phantom replacement and unlink.
      if (color(p) === Color.BLACK) {
        this.fixAfterDeletion(p, ix)
      }

      if (parent(p) !== 0) {
        if (p === left(parent(p))) {
          setLeft(parent(p), 0)
        } else if (p === right(parent(p))) {
          setRight(parent(p), 0)
        }
        setParent(p, 0)
      }
      this.free(p)
    }
  }

  private get cmpMode () {
    return this.options & ~(Options.TIMESTAMPS)
  }

  private getEntry (key: number, ix: number, hash: number, stamp = false) {
    return this.findEntry(key, ix, hash, stamp)
  }

  private findEntry (key: number, ix: number, hash: number, stamp: boolean) {
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

  private root (index: number) {
    return this.table.readInt32LE(index << 2)
  }

  private setRoot (index: number, offset: number) {
    this.table.writeInt32LE(offset, index << 2)
  }

  private get tabLength () {
    return this.table.byteLength >>> 2
  }

  private indexFor (hash: number) {
    return Math.abs(hash % this.tabLength)
  }

  private metadaOverhead () {
    return this.timestamps() ? 4 : 0
  }

  private timestamps () {
    return (this.options & Options.TIMESTAMPS) !== 0
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

  private key (p: number) {
    return this.storage.getInt(p)
  }

  private hash (p: number) {
    return this.storage.getInt(p + 4)
  }

  private left (p: number): number {
    return this.storage.getInt(p + 8)
  }

  private right (p: number): number {
    return this.storage.getInt(p + 12)
  }

  private parent (e: number) {
    return this.storage.getInt(e + 16)
  }

  private color (e: number): Color {
    return this.storage.getByte(e + 20) ? Color.BLACK : Color.RED
  }

  private setKey (p: number, k: number) {
    this.storage.putInt(p, k)
  }

  private setHash (p: number, h: number) {
    this.storage.putInt(p + 4, h)
  }

  private setLeft (p: number, v: number) {
    this.storage.putInt(p + 8, v)
  }

  private setRight (p: number, val: number): void {
    this.storage.putInt(p + 12, val)
  }

  private setParent (e: number, val: number) {
    this.storage.putInt(e + 16, val)
  }

  private setColor (e: number, val: Color) {
    if (e > 0) { // guard due to unconditional call in fixAfterInsertion
      this.storage.putByte(e + 20, val)
    }
  }

  private cmp (p: number, key: number) {
    return cmpNum(key, this.key(p))
  }

  private readVal (p: number): V | null {
    return this.vs.deserialize(Pipe.shared.source(this.storage.slice(p, this.baseEntrySize, this.maxValLen(p))))
  }

  private setValue (e: number, key: number, ix: number, val: V) {
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
    const key = this.key(p)
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

  private free (e: number) {
    if (!e || e < 0) {
      throw new Error(`Invalid pointer ${e}`)
    }
    this.storage.free(e)
  }

  private maxValLen (e: number) {
    return this.storage.sizeOf(e) - this.baseEntrySize
  }

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

  private fixAfterDeletion (x: number, ix: number) {
    const {
      colorOf, leftOf, parentOf, rightOf,
      root, rotateLeft, rotateRight,
      setColor
    } = this

    while (x !== root(ix) && colorOf(x) === Color.BLACK) {
      if (x === leftOf(parentOf(x))) {
        let sib = rightOf(parentOf(x))

        if (colorOf(sib) === Color.RED) {
          setColor(sib, Color.BLACK)
          setColor(parentOf(x), Color.RED)
          rotateLeft(parentOf(x), ix)
          sib = rightOf(parentOf(x))
        }

        if (colorOf(leftOf(sib)) === Color.BLACK && colorOf(rightOf(sib)) === Color.BLACK) {
          setColor(sib, Color.RED)
          x = parentOf(x)
        } else {
          if (colorOf(rightOf(sib)) === Color.BLACK) {
            setColor(leftOf(sib), Color.BLACK)
            setColor(sib, Color.RED)
            rotateRight(sib, ix)
            sib = rightOf(parentOf(x))
          }
          setColor(sib, colorOf(parentOf(x)))
          setColor(parentOf(x), Color.BLACK)
          setColor(rightOf(sib), Color.BLACK)
          rotateLeft(parentOf(x), ix)
          x = root(ix)
        }
      } else { // symmetric
        let sib = leftOf(parentOf(x))

        if (colorOf(sib) === Color.RED) {
          setColor(sib, Color.BLACK)
          setColor(parentOf(x), Color.RED)
          rotateRight(parentOf(x), ix)
          sib = leftOf(parentOf(x))
        }

        if (colorOf(rightOf(sib)) === Color.BLACK && colorOf(leftOf(sib)) === Color.BLACK) {
          setColor(sib, Color.RED)
          x = parentOf(x)
        } else {
          if (colorOf(leftOf(sib)) === Color.BLACK) {
            setColor(rightOf(sib), Color.BLACK)
            setColor(sib, Color.RED)
            rotateLeft(sib, ix)
            sib = leftOf(parentOf(x))
          }
          setColor(sib, colorOf(parentOf(x)))
          setColor(parentOf(x), Color.BLACK)
          setColor(leftOf(sib), Color.BLACK)
          rotateRight(parentOf(x), ix)
          x = root(ix)
        }
      }
    }

    setColor(x, Color.BLACK)
  }

  private fixAfterInsertion (p: number, ix: number) {
    const {
      color, colorOf, leftOf,
      parent, parentOf, rightOf,
      root, rotateLeft, rotateRight,
      setColor
    } = this

    setColor(p, Color.RED)

    while (p !== 0 && p !== root(ix) && color(parent(p)) === Color.RED) {
      if (parentOf(p) === leftOf(parentOf(parentOf(p)))) {
        const y = rightOf(parentOf(parentOf(p)))
        if (colorOf(y) === Color.RED) {
          setColor(parentOf(p), Color.BLACK)
          setColor(y, Color.BLACK)
          setColor(parentOf(parentOf(p)), Color.RED)
          p = parentOf(parentOf(p))
        } else {
          if (p === rightOf(parentOf(p))) {
            p = parentOf(p)
            rotateLeft(p, ix)
          }
          setColor(parentOf(p), Color.BLACK)
          setColor(parentOf(parentOf(p)), Color.RED)
          rotateRight(parentOf(parentOf(p)), ix)
        }
      } else {
        const y = leftOf(parentOf(parentOf(p)))
        if (colorOf(y) === Color.RED) {
          setColor(parentOf(p), Color.BLACK)
          setColor(y, Color.BLACK)
          setColor(parentOf(parentOf(p)), Color.RED)
          p = parentOf(parentOf(p))
        } else {
          if (p === leftOf(parentOf(p))) {
            p = parentOf(p)
            rotateRight(p, ix)
          }
          setColor(parentOf(p), Color.BLACK)
          setColor(parentOf(parentOf(p)), Color.RED)
          rotateLeft(parentOf(parentOf(p)), ix)
        }
      }
    }
    setColor(root(ix), Color.BLACK)
  }

  rotateRight (p: number, ix: number) {
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

  rotateLeft (p: number, ix: number) {
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
}
