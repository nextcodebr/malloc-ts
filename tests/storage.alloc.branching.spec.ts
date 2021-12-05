import { NewStorage } from '@/malloc'
// import { logg } from '@/log'
import { range } from './util'

describe('Test Allocate and Free Chunks of random size', () => {
  it('Will Allocate Large Chunk, Free with Fragmentation and Reconcile Large', () => {
    const large = 64 * 1024

    const storage = NewStorage(0)

    const [p0, p1, p2] = [...range(0, 3)].map(_ => storage.malloc(large))

    if (!p0 || !p1 || !p2) {
      throw new Error(`Should have allocated ${large}`)
    }

    storage.free(p1.offset)

    // expected branch: splitFromTree
    const p4 = storage.malloc(large)

    expect(p4?.offset).toBe(p1.offset)
  })

  it('Will Allocate Large Chunk, Free with fragmentation and Reconcile Small', () => {
    const small = 8
    const large = 64 * 1024

    const storage = NewStorage(0)

    const [p0, p1, p2] = [...range(0, 3)].map(_ => storage.malloc(large))

    if (!p0 || !p1 || !p2) {
      throw new Error(`Should have allocated ${large}`)
    }

    storage.free(p1.offset)

    // expected branch: splitSmallFromTree
    const first = storage.allocate(small)
    expect(first).toBeGreaterThan(0)
    expect(first).toBe(p1.offset)

    let contiguous = 1
    let prev = first
    let after = -1
    for (; ;) {
      // branch: splitFromDesignatedVictim until exhausted, then splitFromTop
      const next = storage.allocate(small)
      if ((next - prev) > 16) {
        after = next
        break
      }
      prev = next
      contiguous++
    }

    /*
     * (size of freed large chunk) / (overhead + size of small chunk).
     * Both being 16-byte aligned, we expect the freed large chunk to be
     * fully consumed
     */
    const exp = ~~(large / (8 + small))
    expect(contiguous).toBe(exp)

    expect(after).toBeGreaterThan(0)
    // after is from topChunk, should be contiguous to p2
    expect(after - (p2.offset + storage.sizeOf(p2.offset))).toBe(8)
  })

  it('Will Allocate Small Chunks, Free with fragmentation and Reconcile Small', () => {
    const small = 8

    const storage = NewStorage(0)

    const [p0, p1, p2] = [...range(0, 3)].map(_ => storage.malloc(small))

    if (!p0 || !p1 || !p2) {
      throw new Error(`Should have allocated ${small}`)
    }

    storage.free(p1.offset)

    // expected branch: allocateFromSmallBin
    const first = storage.allocate(small)
    expect(first).toBeGreaterThan(0)
    expect(first).toBe(p1.offset)

    let contiguous = 1
    let prev = first
    let after = -1
    for (; ;) {
      // branch: splitFromTop, no contiguous space
      const next = storage.allocate(small)
      if ((next - prev) > 16) {
        after = next
        break
      }
      prev = next
      contiguous++
    }

    const exp = 1
    expect(contiguous).toBe(exp)

    expect(after).toBeGreaterThan(0)
    // after is from topChunk, should be contiguous to p2
    expect(after - (p2.offset + storage.sizeOf(p2.offset))).toBe(8)
  })

  it('Will Fail with request 32-bit overflow', () => {
    const storage = NewStorage(0)

    expect(storage.allocate(16)).toBeGreaterThan(0)
    // fast path
    expect(storage.allocate(2 * 1024 * 1024 * 1024)).toBe(-1)

    expect(() => storage.allocate(2147483584 - 1)).toThrow(Error)
  })
})
