import { SinglePageStorage } from '@/malloc/single.page.storage'

type VO = {
  age: number
  name: string
}

describe('Test Storage (Coverage)', () => {
  const storage = new SinglePageStorage(0)

  const p = storage.allocate(32)

  storage.putByte(p, 4)
  expect(storage.getByte(p)).toBe(4)

  storage.putShort(p + 1, 2910)
  expect(storage.getByte(p)).toBe(4)
  expect(storage.getShort(p + 1)).toBe(2910)

  storage.putLongUnsafe(p + 1 + 2, 12345678987654321n)
  expect(storage.getByte(p)).toBe(4)
  expect(storage.getShort(p + 1)).toBe(2910)
  expect(storage.getLongUnsafe(p + 1 + 2)).toBe(12345678987654321n)

  storage.putLong(p + 1 + 2 + 8, 999999999999n)
  expect(storage.getByte(p)).toBe(4)
  expect(storage.getShort(p + 1)).toBe(2910)
  expect(storage.getLongUnsafe(p + 1 + 2)).toBe(12345678987654321n)
  expect(storage.getLongUnsafe(p + 1 + 2 + 8)).toBe(999999999999n)

  expect(storage.malloc(2 * 1024 * 1024 * 1024)).toBeNull()

  expect(storage.slice(p).byteLength).toEqual(storage.sizeOf(p))
  expect(storage.slice(p, undefined).byteLength).toEqual(storage.sizeOf(p))
  expect(storage.slice(p, 0).byteLength).toEqual(storage.sizeOf(p))
  expect(() => storage.slice(p, -1)).toThrow(Error)
  expect(() => storage.slice(p, 0, 1000)).toThrow(Error)
  expect(() => (storage.write(p, 0, Buffer.alloc(64)) as unknown) as any).toThrow(Error)
  expect(() => (storage.write(p, -1, Buffer.alloc(1)) as unknown) as any).toThrow(Error)

  const img = Buffer.allocUnsafe(storage.imageSize)
  storage.storeOn(img)
  const copy = SinglePageStorage.load(img, true)
  const noCopy = SinglePageStorage.load(img, false)

  expect(copy.getByte(p)).toBe(4)
  expect(copy.getShort(p + 1)).toBe(2910)
  expect(copy.getLongUnsafe(p + 1 + 2)).toBe(12345678987654321n)
  expect(copy.getLongUnsafe(p + 1 + 2 + 8)).toBe(999999999999n)

  expect(noCopy.getByte(p)).toBe(4)
  expect(noCopy.getShort(p + 1)).toBe(2910)
  expect(noCopy.getLongUnsafe(p + 1 + 2)).toBe(12345678987654321n)
  expect(noCopy.getLongUnsafe(p + 1 + 2 + 8)).toBe(999999999999n)
})

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
