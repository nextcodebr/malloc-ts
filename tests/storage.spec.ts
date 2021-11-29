import { SinglePageStorage } from '@/malloc/single.page.storage'

type VO = {
  age: number
  name: string
}

describe('Test Storage', () => {
  const storage = new SinglePageStorage(0)
  let offsets: number[] = []

  it('Will Allocate And Store', () => {
    for (let i = 0; i < 1000; i++) {
      const vo: VO = {
        age: i,
        name: `John Armless#${i}`
      }
      const buff = Buffer.from(JSON.stringify(vo))
      const payload = Buffer.alloc(4 + buff.byteLength)
      payload.writeUInt32LE(buff.byteLength)
      buff.copy(payload, 4)

      const offset = storage.allocate(payload.byteLength)
      expect(storage.sizeOf(offset)).toBeGreaterThanOrEqual(buff.byteLength)

      offsets.push(offset)
      const slice = storage.slice(offset, 0, payload.byteLength)
      payload.copy(slice)
    }
  })

  it('Will Slice Correctly', () => {
    for (let i = 0; i < 1000; i++) {
      const offset = offsets[i]
      let slice = storage.slice(offset)
      const length = slice.readUInt32LE(0)
      slice = slice.slice(4, 4 + length)

      const exp: VO = {
        age: i,
        name: `John Armless#${i}`
      }

      const found = JSON.parse((slice as unknown) as string) as VO

      expect(found.age).toBe(exp.age)
      expect(found.name).toBe(exp.name)
    }
  })

  it('Will Free all', () => {
    for (const off of offsets) {
      expect(storage.free(off)).toBeTruthy()
    }
    offsets = []
  })
})
