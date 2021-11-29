import { check, ISegment, Options } from './segment'
import { Segment } from './obj.segment'
import { Int32Segment } from './int32.segment'
import { Serializer } from '../../io'

export interface IMap<K, V> {
  put: (key: K, val: V, returnOld?: boolean) => V | null

  putIfAbsent: (key: K, val: V, returnOld?: boolean) => V | null

  remove: (key: K, returnOld?: boolean) => V | null

  get: (key: K, stamp?: false) => V | null
}

abstract class BaseMap<K, V> implements IMap<K, V> {
  protected readonly hash: (k: K) => number
  protected readonly segments: Array<ISegment<K, V>>

  protected abstract createSegments (segments: number, tableSize: number, ks: Serializer<K>, vs: Serializer<V>, options: number): Array<ISegment<K, V>>

  constructor (segments: number, tableSize: number, hash: (k: K) => number, ks: Serializer<K>, vs: Serializer<V>, timestamps?: boolean) {
    this.hash = hash
    let opts = Options.HASH_THEN_BYTE_ORDER
    if (timestamps) {
      opts |= Options.TIMESTAMPS
    }
    this.segments = this.createSegments(segments, tableSize, ks, vs, opts)
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
}

class BigMap<K, V> extends BaseMap<K, V> {
  protected createSegments<K, V> (segments: number, tableSize: number, ks: Serializer<K>, vs: Serializer<V>, options: number) {
    const rv: Array<Segment<K, V>> = []

    for (let i = 0; i < segments; i++) {
      rv.push(new Segment(ks, vs, tableSize, options))
    }

    return rv
  }
}

class Int32Map<V> extends BaseMap<number, V> {
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

export const NewBigInt32Map = <V> (segments: number, vs: Serializer<V>, opts = { tableSize: 4999, timestamps: false }) => {
  const unused = (undefined as unknown) as Serializer<number>
  return new Int32Map(segments, opts.tableSize, v => v, unused, vs, opts.timestamps)
}
