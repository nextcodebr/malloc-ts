import { Serializer, Pipe } from '../../io'

export interface ISegment<K, V> {
  get: (hash: number, key: K, touch?: boolean) => V | null

  put: (hash: number, key: K, value: V, returnOld: boolean, onlyIfAbsent: boolean, map?: (k: K) => V) => V | null

  remove: (hash: number, key: K, returnOld: boolean) => V | null
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
