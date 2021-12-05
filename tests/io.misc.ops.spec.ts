import { copyOf, inflate, Pipe, Sink, Source, sync } from '@/io'
import { URL } from 'url'
import { existsSync, readFileSync } from 'fs'
import { join } from 'path'
import { tmp } from './util'

describe('Core IO', () => {
  it('Will fail on no file protocol', () => {
    expect(() => inflate(new URL('http://err'))).toThrow(Error)
  })

  it('Sink can use mark', () => {
    const buff = Buffer.alloc(256)
    const source = new Source(buff)
    const sink = new Sink(16)

    sink.writeUTF('foo')
    sink.writeUTF('bar')

    sink.drainTo(buff)

    expect(source.readUTF()).toBe('foo')

    source.markPos()

    expect(source.readUTF()).toBe('bar')

    source.reset()

    expect(source.readUTF()).toBe('bar')
  })

  it('Sink can slice either with copy true/false', () => {
    const buff = Buffer.alloc(256)
    const source = new Source(buff)
    const sink = new Sink(16)

    sink.writeInt32(42)
    sink.drainTo(buff)

    const nocp = source.slice(4, false)
    const cp = source.reset().slice(4, true)

    expect(cp !== nocp).toBeTruthy()
    expect(cp).toStrictEqual(nocp)
  })

  it('Slice with 0 len will always return same ref', () => {
    const buff = Buffer.alloc(256)
    const source = new Source(buff)

    const nocp = source.slice(0, false)
    const cp = source.reset().slice(0, true)

    expect(cp !== nocp).toBeFalsy()
  })

  it('Cached reuse', () => {
    const s0 = Pipe.shared.sink(Buffer.alloc(32))
    const s1 = Pipe.shared.sink(Buffer.alloc(32))

    expect(s0 === s1).toBeTruthy()
  })

  it('Bounds', () => {
    const source = Pipe.shared.source(Buffer.alloc(32))
    expect(source.available).toBe(32)
    source.readInt32()
    expect(source.available).toBe(28)
  })

  it('Wrong copy usage', () => {
    expect(() => copyOf(Buffer.alloc(16), 2, 1)).toThrow(Error)
  })

  it('Can resolve local URL', () => {
    const len = inflate(new URL(join('file://', __dirname, 'io.misc.ops.spec.ts'))).byteLength
    expect(len).toBeGreaterThan(0)
  })

  it('Can sync without existing dir', () => {
    const buffer = inflate(new URL(join('file://', __dirname, 'io.misc.ops.spec.ts')))

    const dir = join(tmp(), 'foo')
    const file = join(dir, 'bar')

    expect(existsSync(dir)).toBeFalsy()

    sync(buffer, file)

    const written = readFileSync(file)

    expect(written).toStrictEqual(buffer)
  })
})
