import { check, ISegment, Options } from './segment'
import { ObjSegment } from './obj.segment'
import { Int32Segment } from './int32.segment'
import { inflate, isBuffer, Serializer, sync } from '../../io'
import { PathLike } from 'fs'

export interface IMap<K, V> {
  put: (key: K, val: V, returnOld?: boolean) => V | null

  putIfAbsent: (key: K, val: V, returnOld?: boolean) => V | null

  remove: (key: K, returnOld?: boolean) => V | null

  get: (key: K, stamp?: false) => V | null

  keys: () => Generator<K, void, unknown>

  values: () => Generator<V, void, unknown>

  entries: () => Generator<{
    key: K
    value: V
  }, void, unknown>

  imageSize: number

  size: number

  serialize: () => Buffer

  storeOn: (dst: Buffer) => void

  saveOn: (dst: PathLike) => void
}

type MapOpts = {
  tableSize: number
  flags: number
}

abstract class BaseMap<K, V> implements IMap<K, V> {
  protected readonly hash: (k: K) => number
  protected readonly segments: Array<ISegment<K, V>>
  protected readonly opts: MapOpts

  protected abstract createSegments (segments: number, tableSize: number, ks: Serializer<K>, vs: Serializer<V>, options: number): Array<ISegment<K, V>>

  constructor (segments: number, tableSize: number, hash: (k: K) => number, ks: Serializer<K>, vs: Serializer<V>, timestamps?: boolean) {
    this.hash = hash
    let flags = Options.HASH_THEN_BYTE_ORDER
    if (timestamps) {
      flags |= Options.TIMESTAMPS
    }
    this.opts = { tableSize, flags }
    this.segments = this.createSegments(segments, tableSize, ks, vs, flags)
  }

  public put (key: K, val: V, returnOld?: boolean) {
    const hash = Math.abs(this.hash(check(key)))

    return this.segmentFor(hash).put(hash, key, val, returnOld ?? false, false)
  }

  public get (key: K, stamp?: false) {
    const hash = Math.abs(this.hash(check(key)))

    return this.segmentFor(hash).get(hash, key, stamp)
  }

  public putIfAbsent (key: K, val: V, returnOld = false) {
    const hash = Math.abs(this.hash(check(key)))

    return this.segmentFor(hash).put(hash, key, val, returnOld, true)
  }

  public remove (key: K, returnOld = false): V | null {
    const hash = Math.abs(this.hash(check(key)))

    return this.segmentFor(hash).remove(hash, key, returnOld)
  }

  private segmentFor (hash: number): ISegment<K, V> {
    const s = this.segments
    return s[hash % s.length]
  }

  public * keys () {
    for (const s of this.segments) {
      for (const key of s.keys()) {
        yield key
      }
    }
  }

  public * values () {
    for (const s of this.segments) {
      for (const val of s.values()) {
        yield val
      }
    }
  }

  public * entries () {
    for (const s of this.segments) {
      for (const e of s.entries()) {
        yield e
      }
    }
  }

  public get imageSize () {
    return 12 + this.segments.reduce((l: number, s) => l + s.imageSize, 0)
  }

  public get size () {
    return this.segments.reduce((l: number, s) => l + s.size, 0)
  }

  public serialize () {
    const rv = Buffer.allocUnsafe(this.imageSize)
    this.storeOn(rv)
    return rv
  }

  public storeOn (dst: Buffer) {
    dst.writeUInt32LE(this.segments.length, 0)
    dst.writeUInt32LE(this.opts.tableSize, 4)
    dst.writeInt32LE(this.opts.flags, 8)
    let offset = 12
    for (const s of this.segments) {
      s.storeOn(dst.slice(offset))
      offset += s.imageSize
    }
  }

  public saveOn (dst: PathLike) {
    if (isBuffer(dst)) {
      this.storeOn(dst as Buffer)
    } else {
      sync(this.serialize(), dst as string)
    }
  }
}

class BigMap<K, V> extends BaseMap<K, V> {
  static load<L, R> (src: PathLike, hash: (k: L) => number, ks: Serializer<L>, vs: Serializer<R>, copy = false) {
    const buf = inflate(src)
    const nsegs = buf.readUInt32LE(0)

    const opts: MapOpts = {
      tableSize: buf.readUInt32LE(4),
      flags: buf.readInt32LE(8)
    }

    const segments: Array<ISegment<L, R>> = []

    let offset = 12

    for (let i = 0; i < nsegs; i++) {
      const slice = buf.slice(offset)
      segments[i] = ObjSegment.load(slice, ks, vs, opts.flags, copy)
      offset += segments[i].imageSize
    }

    const rv = Object.setPrototypeOf({ hash, segments, opts }, BigMap.prototype)

    return rv as BigMap<L, R>
  }

  protected createSegments<K, V> (segments: number, tableSize: number, ks: Serializer<K>, vs: Serializer<V>, options: number) {
    const rv: Array<ObjSegment<K, V>> = []

    for (let i = 0; i < segments; i++) {
      rv.push(new ObjSegment(ks, vs, tableSize, options))
    }

    return rv
  }
}

class Int32Map<V> extends BaseMap<number, V> {
  static load<L, R> (src: PathLike, vs: Serializer<R>, copy = false) {
    const buf = inflate(src)
    const nsegs = buf.readUInt32LE(0)

    const opts: MapOpts = {
      tableSize: buf.readUInt32LE(4),
      flags: buf.readInt32LE(8)
    }

    const hash = opts.tableSize === 1 ? (_: number) => 0 : (k: number) => k

    const segments: Array<ISegment<L, R>> = []

    let offset = 12

    for (let i = 0; i < nsegs; i++) {
      const slice = buf.slice(offset)
      segments[i] = Int32Segment.load(slice, vs, opts.flags, copy)
      offset += segments[i].imageSize
    }

    const rv = Object.setPrototypeOf({ hash, segments, opts }, BigMap.prototype)

    return rv as BigMap<L, R>
  }

  protected createSegments<V> (segments: number, tableSize: number, _ks: Serializer<number>, vs: Serializer<V>, options: number) {
    const rv: Array<ISegment<number, V>> = []

    for (let i = 0; i < segments; i++) {
      rv.push(new Int32Segment(vs, tableSize, options))
    }

    return rv
  }
}

export type ExtendedOptions = {
  tableSize: number
  timestamps: boolean
}

export const NewBigMap = <K, V> (segments: number, hash: (k: K) => number, ks: Serializer<K>, vs: Serializer<V>, opts = { tableSize: 4999, timestamps: false }): IMap<K, V> => {
  return new BigMap(segments, opts.tableSize, hash, ks, vs, opts.timestamps)
}

export const NewTreeMap = <K, V> (ks: Serializer<K>, vs: Serializer<V>, timestamps?: boolean): IMap<K, V> => {
  const opts = {
    tableSize: 1,
    timestamps: !!timestamps
  }
  return NewBigMap(1, k => 0, ks, vs, opts)
}

export const NewInt32BigMap = <V> (segments: number, vs: Serializer<V>, opts = { tableSize: 4999, timestamps: false }): IMap<number, V> => {
  const unused = (undefined as unknown) as Serializer<number>
  const hash = opts.tableSize === 1 ? (_: number) => 0 : (k: number) => k
  return new Int32Map(segments, opts.tableSize, hash, unused, vs, opts.timestamps)
}

export const NewInt32TreeMap = <V> (vs: Serializer<V>, timestamps?: boolean): IMap<number, V> => {
  const opts = {
    tableSize: 1,
    timestamps: !!timestamps
  }
  return NewInt32BigMap(1, vs, opts)
}

export const LoadBigMap = <K, V> (src: PathLike, hash: (k: K) => number, ks: Serializer<K>, vs: Serializer<V>, copy?: boolean): IMap<K, V> => {
  return BigMap.load(src, hash, ks, vs, copy)
}

export const LoadInt32BigMap = <V> (src: PathLike, vs: Serializer<V>, copy?: boolean): IMap<number, V> => {
  return Int32Map.load(src, vs, copy)
}
